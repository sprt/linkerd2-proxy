extern crate hyper_balance;
extern crate tower_balance;
extern crate tower_discover;

use futures::{Async, Future, Poll};
use hyper::body::Payload;
use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use self::tower_discover::{Change, Discover};

pub use self::hyper_balance::{PendingUntilFirstData, PendingUntilFirstDataBody};
pub use self::tower_balance::{choose::PowerOfTwoChoices, load::WithPeakEwma, Balance};

use http;
use svc;

/// Configures a stack to resolve `T` typed targets to balance requests over
/// `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct Layer<A, B, C> {
    decay: Duration,
    default_rtt: Duration,
    fallback: C,
    _marker: PhantomData<fn(A) -> B>,
}

/// Resolves `T` typed targets to balance requests over `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct MakeSvc<M, A, B, C> {
    decay: Duration,
    default_rtt: Duration,
    inner: M,
    fallback: C,
    _marker: PhantomData<fn(A) -> B>,
}

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
pub struct Error;

// === impl Layer ===

pub fn layer<A, B, C>(default_rtt: Duration, decay: Duration, fallback: C) -> Layer<A, B, C>
where
    C: svc::Service<http::Request<A>, Response = http::Response<B>> + Clone + Send + Sync + 'static,
{
    Layer {
        decay,
        default_rtt,
        fallback,
        _marker: PhantomData,
    }
}

impl<A, B, C: Clone> Clone for Layer<A, B, C> {
    fn clone(&self) -> Self {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            fallback: self.fallback.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, A, B, C> svc::Layer<M> for Layer<A, B, C>
where
    A: Payload,
    B: Payload,
    C: Clone + Send + Sync,
{
    type Service = MakeSvc<M, A, B, C>;

    fn layer(&self, inner: M) -> Self::Service {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            inner,
            fallback: self.fallback.clone(),
            _marker: PhantomData,
        }
    }
}

// === impl MakeSvc ===

impl<M: Clone, A, B, C: Clone> Clone for MakeSvc<M, A, B, C> {
    fn clone(&self) -> Self {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            inner: self.inner.clone(),
            fallback: self.fallback.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, M, A, B, C> svc::Service<T> for MakeSvc<M, A, B, C>
where
    M: svc::Service<T>,
    M::Response: Discover,
    <M::Response as Discover>::Service:
        svc::Service<http::Request<A>, Response = http::Response<B>>,
    A: Payload,
    B: Payload,
    C: svc::Service<http::Request<A>, Response = http::Response<B>> + Clone + Send + Sync + 'static,
{
    type Response = Service<
        C,
        Balance<WithPeakEwma<WithEmpty<M::Response>, PendingUntilFirstData>, PowerOfTwoChoices>,
    >;
    type Error = M::Error;
    type Future = MakeSvc<M::Future, A, B, C>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.inner.poll_ready()
    }

    fn call(&mut self, target: T) -> Self::Future {
        let inner = self.inner.call(target);

        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            inner,
            fallback: self.fallback.clone(),
            _marker: PhantomData,
        }
    }
}

impl<F, A, B, C> Future for MakeSvc<F, A, B, C>
where
    F: Future,
    F::Item: Discover,
    <F::Item as Discover>::Service: svc::Service<http::Request<A>, Response = http::Response<B>>,
    A: Payload,
    B: Payload,
    C: svc::Service<http::Request<A>, Response = http::Response<B>> + Clone + Send + Sync + 'static,
{
    type Item = Service<
        C,
        Balance<WithPeakEwma<WithEmpty<F::Item>, PendingUntilFirstData>, PowerOfTwoChoices>,
    >;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let discover = try_ready!(self.inner.poll());
        let (discover, is_empty) = WithEmpty::new(discover);
        let instrument = PendingUntilFirstData::default();
        let loaded = WithPeakEwma::new(discover, self.default_rtt, self.decay, instrument);
        let balance = Balance::p2c(loaded);
        Ok(Async::Ready(Service {
            balance,
            is_empty,
            fallback: self.fallback.clone(),
        }))
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
    A: svc::Service<R> + Clone + Send + Sync + 'static,
    B: svc::Service<R, Response = A::Response> + Clone + Send + Sync + 'static,
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
