extern crate hyper_balance;
extern crate tower_balance;
extern crate tower_discover;

use std::{
    marker::PhantomData,
    time::Duration,
    mem, fmt,
};

use futures::{future, Async, Future, Poll};
use hyper::body::Payload;

use self::tower_discover::Discover;

pub use self::hyper_balance::{PendingUntilFirstData, PendingUntilFirstDataBody};
pub use self::tower_balance::{choose::PowerOfTwoChoices, load::WithPeakEwma, Balance};

use proxy::{
    resolve::{HasEndpointStatus, EndpointStatus},
    http::router,
};
use http;
use svc::{self, layer::util::Identity};

/// Configures a stack to resolve `T` typed targets to balance requests over
/// `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct Layer<A, B, D, R> {
    decay: Duration,
    default_rtt: Duration,
    discover: D,
    fallback_router: R,
    _marker: PhantomData<fn(A) -> B>,
}

/// Resolves `T` typed targets to balance requests over `M`-typed endpoint stacks.
#[derive(Debug)]
pub struct MakeSvc<M, A, B, R> {
    decay: Duration,
    default_rtt: Duration,
    balance: M,
    router: R,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Debug)]
pub struct MakeFuture<M, A, B, R>
where
    M: Future,
    // R: Future,
{
    decay: Duration,
    default_rtt: Duration,
    balance: Making<M>,
    router: R,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Debug)]
pub struct Service<S, F> {
    balance: S,
    router: F,
    status: EndpointStatus,
}

#[derive(Debug)]
pub enum Error<B, F> {
    Balance(B),
    Fallback(F),
}

enum Making<A: Future> {
    NotReady(A),
    Ready(A::Item),
    Over,
}

// === impl Layer ===

pub fn layer<A, B>(default_rtt: Duration, decay: Duration) -> Layer<A, B, Identity, Identity> {
    Layer {
        decay,
        default_rtt,
        discover: svc::layer::util::Identity::new(),
        fallback_router: svc::layer::util::Identity::new(),
        _marker: PhantomData,
    }
}

impl<A, B, D: Clone, R: Clone> Clone for Layer<A, B, D, R> {
    fn clone(&self) -> Self {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            discover: self.discover.clone(),
            fallback_router: self.fallback_router.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A, B, D, R> Layer<A, B, D, R> {
    pub fn with_discover<D2>(self, discover: D2) -> Layer<A, B, D2, R> {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            discover,
            fallback_router: self.fallback_router,
            _marker: PhantomData,
        }
    }

    pub fn with_fallback<Rec>(self, recognize: Rec) -> Layer<A, B, D, router::Layer<http::Request<A>, Rec>>
    where
        Rec: router::Recognize<http::Request<A>> + Clone + Send + Sync + 'static,
     {
        Layer {
            decay: self.decay,
            default_rtt: self.default_rtt,
            discover: self.discover,
            fallback_router: router::layer(recognize),
            _marker: PhantomData,
        }
    }
}

impl<M, A, B, D, R> svc::Layer<M> for Layer<A, B, D, R>
where
    A: Payload,
    B: Payload,
    D: svc::Layer<M>,
    R: svc::Layer<M>,
{
    type Service = MakeSvc<D::Service, A, B, R::Service>;

    fn layer(&self, inner: M) -> Self::Service {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            balance: self.discover.layer(inner),
            router: self.fallback_router.layer(inner),
            _marker: PhantomData,
        }
    }
}

// === impl MakeSvc ===

impl<M, A, B, R> Clone for MakeSvc<M, A, B, R>
where
    M: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        MakeSvc {
            decay: self.decay,
            default_rtt: self.default_rtt,
            balance: self.balance.clone(),
            router: self.router.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, M, A, B, R> svc::Service<T> for MakeSvc<M, A, B, router::Stack<http::Request<A>, R, M>>
where
    M: svc::Service<T> + Send + 'static,
    M::Response: Discover + HasEndpointStatus,
    <M::Response as Discover>::Service:
        svc::Service<http::Request<A>, Response = http::Response<B>> + Send + 'static,
    <<M::Response as Discover>::Service as
        svc::Service<http::Request<A>>>::Future: Send + 'static,
    A: Payload,
    B: Payload,
    // R: svc::Service<router::Config> + Send + 'static,
    // R::Response: svc::Service<http::Request<A>, Response = http::Response<B>> + Send + 'static,
    // <R::Response as svc::Service<http::Request<A>>>::Future: Send,
    R: router::Recognize<http::Request<A>> + Send + Sync + 'static + Clone,
    M: router::Make<R::Target> + Clone + Send + Sync + 'static,
    M::Value: svc::Service<http::Request<A>, Response = http::Response<B>> + Clone,
    <M::Value as svc::Service<http::Request<A>>>::Error: ::std::error::Error + Send + Sync,
    <router::Service<http::Request<A>, R, M> as svc::Service<http::Request<A>>>::Error: ::std::error::Error + Send + Sync,
    B: Default + Send + 'static,
{
    type Response = Service<Balance<WithPeakEwma<M::Response, PendingUntilFirstData>, PowerOfTwoChoices>, router::Service<http::Request<A>, R, M>>;
    type Error = Error<M::Error, <router::Service<http::Request<A>, R, M> as svc::Service<http::Request<A>>>::Error>;
    type Future = MakeFuture<M::Future, A, B, router::Service<http::Request<A>, R, M>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.balance.poll_ready().map_err(Error::Balance)
    }

    fn call(&mut self, target: T) -> Self::Future {
        let balance = Making::NotReady(self.balance.call(target));
        // TODO: good config
        let router = self.router.make(&router::Config::new("out ep", 10, Duration::from_secs(10)));
        MakeFuture {
            decay: self.decay,
            default_rtt: self.default_rtt,
            balance,
            router,
            _marker: PhantomData,
        }
    }
}

// impl<F, A, B, R> Future for MakeSvc<F, A, B, R>
// where
//     F: Future,
//     F::Item: Discover + HasEndpointStatus,
//     <F::Item as Discover>::Service: svc::Service<http::Request<A>, Response = http::Response<B>>,
//     R: Future
//     A: Payload,
//     B: Payload,

// {
//     type Item = Service<Balance<WithPeakEwma<F::Item, PendingUntilFirstData>, PowerOfTwoChoices>>;
//     type Error = F::Error;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         let discover = try_ready!(self.balance.poll().map_err(Error::Balance));
//         let status = discover.endpoint_status();
//         let instrument = PendingUntilFirstData::default();
//         let loaded = WithPeakEwma::new(discover, self.default_rtt, self.decay, instrument);
//         let balance = Balance::p2c(loaded);
//         Ok(Async::Ready(Service {
//             balance,
//             status
//         }))
//     }
// }


impl<F, A, B, R> Future for MakeFuture<F, A, B, R>
where
    F: Future,
    F::Item: Discover + HasEndpointStatus,
    <F::Item as Discover>::Service: svc::Service<http::Request<A>, Response = http::Response<B>>,
    R: svc::Service<http::Request<A>, Response = http::Response<B>> + Clone,
    A: Payload,
    B: Payload,
{
    type Item = Service<
        Balance<WithPeakEwma<F::Item, PendingUntilFirstData>, PowerOfTwoChoices>,
        R
    >;
    type Error = Error<F::Error, <R as svc::Service<http::Request<A>>>::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.balance.poll().map_err(Error::Balance)? {
            let discover = self.balance.take();
            let status = discover.endpoint_status();
            let instrument = PendingUntilFirstData::default();
            let loaded = WithPeakEwma::new(discover, self.default_rtt, self.decay, instrument);
            let balance = Balance::p2c(loaded);
            Ok(Async::Ready(Service {
                balance,
                status,
                router: self.router.clone(),
            }))
        } else {
            Ok(Async::NotReady)
        }

    }
}

impl<S, A, B, F> svc::Service<http::Request<A>> for Service<S, F>
where
    S: svc::Service<http::Request<A>, Response = http::Response<B>>,
    F: svc::Service<http::Request<A>, Response = http::Response<B>>,
{
    type Response = http::Response<B>;
    type Error = Error<S::Error, F::Error>;
    type Future = future::Either<
        futures::MapErr<
            F::Future,
            fn(<F::Future as Future>::Error) -> Error<S::Error, F::Error>,
        >,
        futures::MapErr<
            S::Future,
            fn(<S::Future as Future>::Error) -> Error<S::Error, F::Error>,
        >
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.balance.poll_ready().map_err(Error::Balance)
    }

    fn call(&mut self, req: http::Request<A>) -> Self::Future {
        if self.status.is_empty() {
            future::Either::A(self.router.call(req).map_err(Error::Fallback))
        } else {
            future::Either::B(self.balance.call(req).map_err(Error::Balance))
        }
    }
}

// === impl Error ===

impl<B, F> fmt::Display for Error<B, F>
where
    B: fmt::Display,
    F: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Balance(ref e) => e.fmt(f),
            Error::Fallback(ref e) => e.fmt(f),
        }
    }
}

impl<B, F> ::std::error::Error for Error<B, F>
where
    B: ::std::error::Error + 'static,
    F: ::std::error::Error + 'static,
{
    fn source(&self) -> Option<&(::std::error::Error + 'static)> {
        match self {
            Error::Balance(ref e) => Some(e),
            Error::Fallback(ref e) => Some(e),
        }
    }
}

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
impl<F: Future> fmt::Debug for Making<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Making::NotReady(_) => fmt::Display::fmt("Making::NotReady(...)", f),
            Making::Ready(_) => fmt::Display::fmt("Making::Ready(...)", f),
            Making::Over => fmt::Display::fmt("Making::Over", f),
        }
    }
}
