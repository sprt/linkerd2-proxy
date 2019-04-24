extern crate hyper_balance;
extern crate tower_balance;
extern crate tower_discover;
extern crate linkerd2_router as rt;

use futures::{Async, Future, Poll};
use hyper::body::Payload;
use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::mem;
use std::time::Duration;

use self::tower_discover::{Change, Discover};

pub use self::hyper_balance::{PendingUntilFirstData, PendingUntilFirstDataBody};
pub use self::tower_balance::{choose::PowerOfTwoChoices, load::WithPeakEwma, Balance};

use proxy::{resolve, http::router};
use http;
use svc;

/// Configures a stack to resolve `T` typed targets to balance requests over
/// `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct Layer<A, B, C, R> {
    decay: Duration,
    default_rtt: Duration,
    resolve: R,
    fallback: C,
    config: router::Config,
    _marker: PhantomData<fn(A) -> B>,
}

/// Resolves `T` typed targets to balance requests over `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct MakeSvc<A, B, C, R> {
    decay: Duration,
    default_rtt: Duration,
    fallback: C,
    resolve: R,
    config: router::Config,
    _marker: PhantomData<fn(A) -> B>,
}

/// Resolves `T` typed targets to balance requests over `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct MakeFuture<A, B, C, R>
where
    C: Future,
    C::Item: fmt::Debug,
    R: Future,
    R::Item: fmt::Debug,
{
    decay: Duration,
    default_rtt: Duration,
    fallback: Making<C>,
    resolve: Making<R>,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Clone)]
pub struct Service<A, B> {
    is_empty: Arc<AtomicBool>,
    balance: B,
    fallback: A,
}

#[derive(Clone, Debug)]
pub struct WithEmpty<D> {
    inner: D,
    is_empty: Arc<AtomicBool>,
    count: usize,
}

#[derive(Clone)]
pub enum ResponseFuture<A, B> {
    Fallback(A),
    Balance(B),
}

#[derive(Debug)]
enum Making<A: Future> {
    NotReady(A),
    Ready(A::Item),
    Over,
}

#[derive(Debug)]
pub struct Error;

// === impl Layer ===

pub fn layer<A, B>(default_rtt: Duration, decay: Duration, config: router::Config) -> Layer<A, B, (), ()> {
    Layer {
        decay,
        default_rtt,
        fallback: (),
        resolve: (),
        config,
        _marker: PhantomData,
    }
}

impl<A, B, C> Layer<A, B, C, ()> {
    pub fn with_resolve<R: svc::Layer<M>, M>(self, resolve: R) -> Layer<A, B, C, R> {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback: self.fallback,
            resolve,
            config: self.config,
            _marker: PhantomData
        }
    }
}

impl<A, B, R> Layer<A, B, (), R> {
    pub fn with_fallback<C: svc::Layer<M>, M>(self, fallback: C) -> Layer<A, B, C, R> {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback,
            resolve: self.resolve,
            config: self.config,
            _marker: PhantomData
        }
    }
}


impl<A, B, C: Clone, R: Clone> Clone for Layer<A, B, C, R> {
    fn clone(&self) -> Self {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback: self.fallback.clone(),
            resolve: self.resolve.clone(),
            config: self.config.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, A, B, C, R> svc::Layer<M> for Layer<A, B, C, R>
where
    A: Payload,
    B: Payload,
    C: svc::Layer<M>,
    C::Service: Send + 'static,
    R: svc::Layer<M>,
    R::Service: Send + 'static,
{
    type Service = MakeSvc<A, B, C::Service, R::Service>;

    fn layer(&self, inner: M) -> Self::Service {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback: self.fallback.layer(inner),
            resolve: self.resolve.layer(inner),
            config: self.config.clone(),
            _marker: PhantomData,
        }
    }
}

// === impl MakeSvc ===

impl<A, B, C: Clone, R: Clone> Clone for MakeSvc<A, B, C, R> {
    fn clone(&self) -> Self {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback: self.fallback.clone(),
            resolve: self.resolve.clone(),
            config: self.config.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, A, B, C, R> svc::Service<T> for MakeSvc<A, B, C, R>
where
    R: svc::Service<T>,
    R::Response: Discover + fmt::Debug,
    R::Future: Send + 'static,
    <R::Future as Future>::Item: fmt::Debug,
    R::Response: Discover,
    <R::Response as Discover>::Service:
        svc::Service<http::Request<A>, Response = http::Response<B>> + fmt::Debug+ Send + Sync + 'static,

    <<R::Response as Discover>::Service as svc::Service<http::Request<A>>>::Future: Send + 'static,
    C: svc::Service<router::Config> + Send + 'static,

    C::Future: Send + 'static,
    <C::Future as Future>::Item: fmt::Debug,
    C::Response:
        svc::Service<http::Request<A>, Response = http::Response<B>> + Send + Sync + 'static,
    <C::Response as svc::Service<http::Request<A>>>::Future: Send + 'static,
    A: Payload,
    B: Payload,
{
    type Response = Service<
        C::Response,
        Balance<WithPeakEwma<WithEmpty<R::Response>, PendingUntilFirstData>, PowerOfTwoChoices>,
    >;
    type Error = Error;
    type Future = MakeFuture<A, B, C::Future, R::Future>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let resolve = self.resolve.poll_ready().map_err(|_| Error);
        let fallback = self.fallback.poll_ready().map_err(|_| Error);
        try_ready!(resolve);
        fallback
    }

    fn call(&mut self, target: T) -> Self::Future {
        let resolve = Making::NotReady(self.resolve.call(target));
        let fallback = Making::NotReady(self.fallback.call(self.config.clone()));

        MakeFuture {
            decay: self.decay,
            default_rtt: self.default_rtt,
            resolve,
            fallback,
            _marker: PhantomData,
        }
    }
}

impl<A, B, C, R> Future for MakeFuture<A, B, C, R>
where
    R: Future,
    R::Item: Discover + fmt::Debug,
    <R::Item as Discover>::Service: svc::Service<http::Request<A>, Response = http::Response<B>>+ Send + Sync + 'static,
    A: Payload,
    B: Payload,
    C: Future,
    C::Item: svc::Service<http::Request<A>, Response = http::Response<B>> + fmt::Debug + Send + Sync + 'static,
{
    type Item = Service<
        C::Item,
        Balance<WithPeakEwma<WithEmpty<R::Item>, PendingUntilFirstData>, PowerOfTwoChoices>,
    >;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.resolve.poll().map_err(|_| Error)? && self.fallback.poll().map_err(|_| Error)? {
            let (discover, is_empty) = WithEmpty::new(self.resolve.take());
            let instrument = PendingUntilFirstData::default();
            let loaded = WithPeakEwma::new(discover, self.default_rtt, self.decay, instrument);
            let balance = Balance::p2c(loaded);
            Ok(Async::Ready(Service {
                balance,
                is_empty,
                fallback: self.fallback.take(),
            }))
        } else {
            Ok(Async::NotReady)
        }

    }
}

// ==== impl WithEmpty ====

impl<D: Discover> Discover for WithEmpty<D> {
    type Key = D::Key;
    type Service = D::Service;
    type Error = D::Error;


    fn poll(&mut self) -> Poll<Change<Self::Key, Self::Service>, Self::Error> {
        let change = try_ready!(self.inner.poll());
        match change {
            Change::Insert(_, _) => {
                self.count += 1;
                self.is_empty.store(false, Ordering::Release);
            }
            Change::Remove(_) => {
                self.count -= 1;
                if self.count == 0 {
                    self.is_empty.store(true, Ordering::Release);
                }
            }
        };
        Ok(Async::Ready(change))
    }
}

impl<D: Discover> WithEmpty<D> {
    fn new(inner: D) -> (Self, Arc<AtomicBool>) {
        let is_empty = Arc::new(AtomicBool::new(true));
        let handle = is_empty.clone();
        let discover = Self {
            inner,
            is_empty,
            count: 0,
        };
        (discover, handle)
    }
}

impl<A, B, R> svc::Service<R> for Service<A, B>
where
    A: svc::Service<R> + Send + Sync + 'static,
    B: svc::Service<R, Response = A::Response> + Send + Sync + 'static,
{
    type Response = A::Response;
    type Error = Error;
    type Future = ResponseFuture<A::Future, B::Future>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let balance_ready = self.balance.poll_ready().map_err(|_| Error)?;
        let fallback_ready = self.fallback.poll_ready().map_err(|_| Error)?;
        if self.is_empty.load(Ordering::Acquire) {
            Ok(fallback_ready)
        } else {
            Ok(balance_ready)
        }
    }

    fn call(&mut self, request: R) -> Self::Future {
        if self.is_empty.load(Ordering::Acquire) {
            ResponseFuture::Fallback(self.fallback.call(request))
        } else {
            ResponseFuture::Balance(self.balance.call(request))
        }
    }
}

impl<A, B> Future for ResponseFuture<A, B>
where
    A: Future,
    B: Future<Item = A::Item>,
{
    type Error = Error;
    type Item = A::Item;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            ResponseFuture::Balance(ref mut f) => f.poll().map_err(|_| Error), // FIXME (eliza): lol,
            ResponseFuture::Fallback(ref mut f) => f.poll().map_err(|_| Error), // FIXME (eliza): lol,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "FIXME(eliza)".fmt(f)
    }
}
impl error::Error for Error {}


impl<A: Future> Making<A> {
    fn poll(&mut self) -> Result<bool, A::Error> {
        let res = match *self {
            Making::NotReady(ref mut a) => a.poll()?,
            Making::Ready(_) => return Ok(true),
            Making::Over => panic!("cannot poll MakeFuture twice"),
        };
        match res {
            Async::Ready(res) => {
                *self = Making::Ready(res);
                Ok(true)
            }
            Async::NotReady => Ok(false),
        }
    }

    fn take(&mut self) -> A::Item {
        match mem::replace(self, Making::Over) {
            Making::Ready(a) => a,
            _ => panic!(),
        }
    }
}
