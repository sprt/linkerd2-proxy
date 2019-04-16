//! A stack module that lazily, dynamically resolves an `Addr` target, via DNS,
//! to determine it's canonical fully qualified domain name.
//!
//! For example, an application may set an authority value like `web:8080` with a
//! resolv.conf(5) search path of `example.com example.net`. In such a case,
//! this module may build its inner stack with either `web.example.com.:8080`,
//! `web.example.net.:8080`, or `web:8080`, depending on the state of DNS.
//!
//! DNS TTLs are honored and, if the resolution changes, the inner stack is
//! rebuilt with the updated value.

use futures::{future, sync::mpsc, Async, Future, Poll, Stream};
use std::{fmt, time::Duration};
use tokio::executor::{DefaultExecutor, Executor};
use tokio_timer::{clock, Delay, Timeout};

use dns;
use svc;
use {Addr, NameAddr};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Duration to wait before polling DNS again after an error (or a NXDOMAIN
/// response with no TTL).
const DNS_ERROR_TTL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
pub struct Layer {
    resolver: dns::Resolver,
    timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct Stack<M> {
    resolver: dns::Resolver,
    inner: M,
    timeout: Duration,
}

/// Trait implemented by types that can be refined into a canonical FQDN.
pub trait Canonicalize {
    /// If this is a name that should be canonicalized, returns the original
    /// unrefined name. Otherwise, if this name should not be canonicalized
    /// (i.e., it's a socket address rather than a DNS name), returns `None`.
    fn uncanonical_name(&self) -> Option<&dns::Name>;

    /// Transforms `self` into canonical form with the provided `canonical`
    /// FQDN.
    ///
    // If `self` does not hold a canonicalizable name (i.e. it is a
    /// socket address), this should simply clone `self` and ignore the
    /// canonical name.
    fn with_canonical(&self, canonical: dns::Name) -> Self;

    /// Returns true if `self` should be canonicalized.
    fn should_canonicalize(&self) -> bool {
        self.uncanonical_name().is_some()
    }
}

pub struct Service<M: svc::Stack<N>, N> {
    rx: mpsc::Receiver<N>,
    stack: M,
    service: Option<M::Value>,
}

struct Task<N> {
    original: N,
    resolved: Cache<N>,
    resolver: dns::Resolver,
    state: State,
    timeout: Duration,
    tx: mpsc::Sender<N>,
}

/// Tracks the state of the last resolution.
#[derive(Debug, Clone, Eq, PartialEq)]
enum Cache<N> {
    /// The service has not yet been notified of a value.
    AwaitingInitial,

    /// The service has been notified with the original value (i.e. due to an
    /// error), and we do not yet have a resolved name.
    Unresolved,

    /// The service was last-notified with this name.
    Resolved(N),
}

enum State {
    Init,
    Pending(Timeout<dns::RefineFuture>),
    ValidUntil(Delay),
}

// === Layer ===

// FIXME the resolver should be abstracted to a trait so that this can be tested
// without a real DNS service.
pub fn layer(resolver: dns::Resolver, timeout: Duration) -> Layer {
    Layer { resolver, timeout }
}

impl<M, N> svc::Layer<N, N, M> for Layer
where
    M: svc::Stack<N> + Clone,
    N: Canonicalize + Clone + Eq + fmt::Display + fmt::Debug + Send + 'static,
{
    type Value = <Stack<M> as svc::Stack<N>>::Value;
    type Error = <Stack<M> as svc::Stack<N>>::Error;
    type Stack = Stack<M>;

    fn bind(&self, inner: M) -> Self::Stack {
        Stack {
            inner,
            resolver: self.resolver.clone(),
            timeout: self.timeout,
        }
    }
}

// === impl Stack ===

impl<M, N> svc::Stack<N> for Stack<M>
where
    M: svc::Stack<N> + Clone,
    N: Canonicalize + Clone + Eq + fmt::Display + fmt::Debug + Send + 'static,
{
    type Value = svc::Either<Service<M, N>, M::Value>;
    type Error = M::Error;

    fn make(&self, name: &N) -> Result<Self::Value, Self::Error> {
        if name.should_canonicalize() {
            let (tx, rx) = mpsc::channel(2);

            DefaultExecutor::current()
                .spawn(Box::new(Task::new(
                    name.clone(),
                    self.resolver.clone(),
                    self.timeout,
                    tx,
                )))
                .expect("must be able to spawn");

            let svc = Service {
                rx,
                stack: self.inner.clone(),
                service: None,
            };
            Ok(svc::Either::A(svc))
        } else {
            self.inner.make(name).map(svc::Either::B)
        }
    }
}

// === impl Task ===

impl<N> Task<N>
where
    N: Canonicalize + Clone + Eq + fmt::Debug,
{
    fn new(
        original: N,
        resolver: dns::Resolver,
        timeout: Duration,
        tx: mpsc::Sender<N>,
    ) -> Self {
        Self {
            original,
            resolved: Cache::AwaitingInitial,
            resolver,
            state: State::Init,
            timeout,
            tx,
        }
    }
}

impl<N> Future for Task<N>
where
    N: Canonicalize + Clone + Eq + fmt::Debug,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        let uncanonical_name = self.original.uncanonical_name()
        .expect("original must be uncanonicalized");
        loop {
            self.state = match self.state {
                State::Init => {
                    let f = self.resolver.refine(uncanonical_name);
                    State::Pending(Timeout::new(f, self.timeout))
                }
                State::Pending(ref mut fut) => {
                    match fut.poll() {
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }
                        Ok(Async::Ready(refine)) => {
                            // If the resolved name is a new name, bind a
                            // service with it and set a delay that will notify
                            // when the resolver should be consulted again.
                            let resolved = self.original.with_canonical(refine.name);
                            if self.resolved.get() != Some(&resolved) {
                                let err = self.tx.try_send(resolved.clone()).err();
                                if err.map(|e| e.is_disconnected()).unwrap_or(false) {
                                    return Ok(().into());
                                }

                                self.resolved = Cache::Resolved(resolved);
                            }

                            State::ValidUntil(Delay::new(refine.valid_until))
                        }
                        Err(e) => {
                            if self.resolved == Cache::AwaitingInitial {
                                // The service needs a value, so we need to
                                // publish the original name so it can proceed.
                                warn!(
                                    "failed to refine {}: {}; using original name",
                                    uncanonical_name,
                                    e,
                                );
                                let err = self.tx.try_send(self.original.clone()).err();
                                if err.map(|e| e.is_disconnected()).unwrap_or(false) {
                                    return Ok(().into());
                                }

                                // There's now no need to re-publish the
                                // original name on subsequent failures.
                                self.resolved = Cache::Unresolved;
                            } else {
                                debug!(
                                    "failed to refresh {}: {}; cache={:?}",
                                    uncanonical_name,
                                    e,
                                    self.resolved,
                                );
                            }

                            let valid_until = e
                                .into_inner()
                                .and_then(|e| match e.kind() {
                                    dns::ResolveErrorKind::NoRecordsFound {
                                        valid_until, ..
                                    } => *valid_until,
                                    _ => None,
                                })
                                .unwrap_or_else(|| clock::now() + DNS_ERROR_TTL);

                            State::ValidUntil(Delay::new(valid_until))
                        }
                    }
                }

                State::ValidUntil(ref mut f) => {
                    match f.poll().expect("timer must not fail") {
                        Async::NotReady => return Ok(Async::NotReady),
                        Async::Ready(()) => {
                            // The last resolution's TTL expired, so issue a new DNS query.
                            State::Init
                        }
                    }
                }
            };
        }
    }
}

impl<N> Cache<N> {
    fn get(&self) -> Option<&N> {
        match self {
            Cache::Resolved(ref r) => Some(&r),
            _ => None,
        }
    }
}

// === impl Service ===

impl<M, Req, Svc, N> svc::Service<Req> for Service<M, N>
where
    M: svc::Stack<N, Value = Svc>,
    M::Error: Into<Error>,
    Svc: svc::Service<Req>,
    Svc::Error: Into<Error>,
    N: Canonicalize + Clone + Eq + fmt::Display,
{
    type Response = <M::Value as svc::Service<Req>>::Response;
    type Error = Error;
    type Future = future::MapErr<
        <M::Value as svc::Service<Req>>::Future,
        fn(<M::Value as svc::Service<Req>>::Error) -> Self::Error,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        while let Ok(Async::Ready(Some(canonical))) = self.rx.poll() {
            debug!("refined: {}", canonical);
            let svc = self.stack.make(&canonical).map_err(Into::into)?;
            self.service = Some(svc);
        }

        match self.service.as_mut() {
            Some(ref mut svc) => svc.poll_ready().map_err(Into::into),
            None => {
                trace!("resolution has not completed");
                Ok(Async::NotReady)
            }
        }
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.service
            .as_mut()
            .expect("poll_ready must be called first")
            .call(req)
            .map_err(Into::into)
    }
}

// === Canonicalize ===

impl Canonicalize for Addr {
    fn uncanonical_name(&self) -> Option<&dns::Name> {
        self.name_addr().map(NameAddr::name)
    }

    fn with_canonical(&self, canonical: dns::Name) -> Self {
        match self {
            Addr::Name(ref original) => Addr::Name(NameAddr::new(canonical, original.port())),
            _ => {
                error!("tried to canonicalize {} with name {}, this is likely a bug", self, canonical);
                self.clone()
            }
        }
    }
}