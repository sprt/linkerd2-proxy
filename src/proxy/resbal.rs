extern crate hyper_balance;
extern crate tower_balance;
extern crate tower_discover;

use futures::{Async, Poll};
use hyper::body::Payload;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;
use std::{error, fmt};
use svc;

pub use self::hyper_balance::{PendingUntilFirstData, PendingUntilFirstDataBody};
pub use self::tower_balance::{choose::PowerOfTwoChoices, load::WithPeakEwma, Balance};
pub use self::tower_discover::Change;

const EWMA_DEFAULT_RTT: Duration = Duration::from_millis(30);
const EWMA_DECAY: Duration = Duration::from_secs(10);

pub trait Resolve<T> {
    type Endpoint;
    type Resolution: Resolution<Endpoint = Self::Endpoint>;

    fn resolve(&self, target: &T) -> Self::Resolution;
}

pub trait Resolution {
    type Endpoint;
    type Error;

    fn poll(&mut self) -> Poll<Update<Self::Endpoint>, Self::Error>;
}

#[derive(Clone, Debug)]
pub enum Update<T> {
    Add(SocketAddr, T),
    Remove(SocketAddr),
}

#[derive(Debug)]
pub struct Layer<R, A, B> {
    resolve: R,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Debug)]
pub struct Stack<R, M, A, B> {
    resolve: R,
    inner: M,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Clone, Debug)]
pub struct Discover<R: Resolution, M: svc::Stack<R::Endpoint>, A, B> {
    resolution: R,
    make: M,
    _marker: PhantomData<fn(A) -> B>,
}

#[derive(Debug)]
pub enum Error<R, M> {
    Resolve(R),
    Stack(M),
}

// ===== impl Layer =====

pub fn layer<T, R, A, B>(resolve: R) -> Layer<R, A, B>
where
    R: Resolve<T> + Clone,
    R::Endpoint: fmt::Debug,
{
    Layer {
        resolve,
        _marker: PhantomData,
    }
}

impl<T, R, M, A, B> svc::Layer<T, R::Endpoint, M> for Layer<R, A, B>
where
    R: Resolve<T> + Clone,
    R::Endpoint: fmt::Debug,
    M: svc::Stack<R::Endpoint> + Clone,
    M::Value: svc::Service<http::Request<A>, Response = http::Response<B>>,
    A: Payload,
    B: Payload,
{
    type Value = <Stack<R, M, A, B> as svc::Stack<T>>::Value;
    type Error = <Stack<R, M, A, B> as svc::Stack<T>>::Error;
    type Stack = Stack<R, M, A, B>;

    fn bind(&self, inner: M) -> Self::Stack {
        Stack {
            resolve: self.resolve.clone(),
            inner,
            _marker: PhantomData,
        }
    }
}

impl<R: Clone, A, B> Clone for Layer<R, A, B> {
    fn clone(&self) -> Self {
        Layer {
            resolve: self.resolve.clone(),
            _marker: PhantomData,
        }
    }
}

// ===== impl Stack =====

impl<T, R, M, A, B> svc::Stack<T> for Stack<R, M, A, B>
where
    R: Resolve<T> + Clone,
    R::Endpoint: fmt::Debug,
    M: svc::Stack<R::Endpoint> + Clone,
    M::Value: svc::Service<http::Request<A>, Response = http::Response<B>>,
    A: Payload,
    B: Payload,
{
    type Value = Balance<
        WithPeakEwma<Discover<R::Resolution, M, A, B>, PendingUntilFirstData>,
        PowerOfTwoChoices,
    >;
    type Error = M::Error;

    fn make(&self, target: &T) -> Result<Self::Value, Self::Error> {
        let resolution = self.resolve.resolve(target);
        let discover = Discover {
            resolution,
            make: self.inner.clone(),
            _marker: PhantomData,
        };
        let instrument = PendingUntilFirstData::default();
        let loaded = WithPeakEwma::new(discover, EWMA_DEFAULT_RTT, EWMA_DECAY, instrument);
        Ok(Balance::p2c(loaded))
    }
}

impl<R: Clone, M: Clone, A, B> Clone for Stack<R, M, A, B> {
    fn clone(&self) -> Self {
        Stack {
            resolve: self.resolve.clone(),
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

// ===== impl Discover =====

impl<R, M, A, B> tower_discover::Discover for Discover<R, M, A, B>
where
    R: Resolution,
    R::Endpoint: fmt::Debug,
    M: svc::Stack<R::Endpoint>,
    M::Value: svc::Service<http::Request<A>, Response = http::Response<B>>,
    A: Payload,
    B: Payload,
{
    type Key = SocketAddr;
    type Service = M::Value;
    type Error = Error<R::Error, M::Error>;

    fn poll(&mut self) -> Poll<Change<Self::Key, Self::Service>, Self::Error> {
        loop {
            let up = try_ready!(self.resolution.poll().map_err(Error::Resolve));
            trace!("watch: {:?}", up);
            match up {
                Update::Add(addr, target) => {
                    let svc = self.make.make(&target).map_err(Error::Stack)?;
                    return Ok(Async::Ready(Change::Insert(addr, svc)));
                }
                Update::Remove(addr) => {
                    return Ok(Async::Ready(Change::Remove(addr)));
                }
            }
        }
    }
}

// ===== impl Error =====

impl<M> fmt::Display for Error<(), M>
where
    M: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Resolve(()) => unreachable!("resolution must succeed"),
            Error::Stack(e) => e.fmt(f),
        }
    }
}

impl<M> error::Error for Error<(), M> where M: error::Error {}
