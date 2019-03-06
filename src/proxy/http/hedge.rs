use std::marker::PhantomData;
use std::time::Duration;

use http::{Request, Response};
use linkerd2_hedge;

use super::retry::TryClone;
use svc;

pub trait Budget: Sized {
    fn has_budget<A>(&self, req: &Request<A>) -> bool;
    fn clone_request<A: TryClone>(&self, req: &Request<A>) -> Option<Request<A>>;
}

pub trait CanHedge {
    type Budget: Budget + Clone;
    fn can_hedge(&self) -> Option<Self::Budget>;
}

#[derive(Clone)]
pub struct Policy<B>(B);

pub type Service<B, Svc> = linkerd2_hedge::Hedge<Policy<B>, Svc>;

pub struct Layer<A, A2> {
    percentile: f32,
    period: Duration,
    _p: PhantomData<fn(A) -> A2>,
}

pub struct Stack<M, A, A2> {
    inner: M,
    percentile: f32,
    period: Duration,
    _p: PhantomData<fn(A) -> A2>,
}

// === impl Policy ===

impl<A, B> linkerd2_hedge::Policy<Request<A>> for Policy<B>
where
    A: TryClone,
    B: Budget,
{
    fn can_retry(&self, req: &Request<A>) -> bool {
        self.0.has_budget(req)
    }

    fn clone_request(&self, req: &Request<A>) -> Option<Request<A>> {
        self.0.clone_request(req)
    }
}

// === impl Stack ===

impl<T, M, A, A2> svc::Stack<T> for Stack<M, A, A2>
where
    T: CanHedge + Clone,
    M: svc::Stack<T>,
    M::Value: svc::Service<Request<A>, Response = Response<A2>> + Clone,
    A: TryClone,
{
    type Value = svc::Either<Service<T::Budget, M::Value>, M::Value>;
    type Error = M::Error;

    fn make(&self, target: &T) -> Result<Self::Value, Self::Error> {
        let inner = self.inner.make(target)?;
        if let Some(budget) = target.can_hedge() {
            trace!("stack is hedgeable");
            Ok(svc::Either::A(linkerd2_hedge::Hedge::new(
                Policy(budget),
                inner,
                self.percentile,
                self.period,
            )))
        } else {
            Ok(svc::Either::B(inner))
        }
    }
}

impl<M: Clone, A, A2> Clone for Stack<M, A, A2> {
    fn clone(&self) -> Self {
        Stack {
            inner: self.inner.clone(),
            percentile: self.percentile,
            period: self.period,
            _p: PhantomData,
        }
    }
}

// === impl Layer ===

pub fn layer<A, A2>(percentile: f32, period: Duration) -> Layer<A, A2> {
    Layer {
        percentile,
        period,
        _p: PhantomData,
    }
}

impl<T, M, A, A2> svc::Layer<T, T, M> for Layer<A, A2>
where
    T: CanHedge + Clone,
    M: svc::Stack<T>,
    M::Value: svc::Service<Request<A>, Response = Response<A2>> + Clone,
    A: TryClone,
{
    type Value = <Stack<M, A, A2> as svc::Stack<T>>::Value;
    type Error = <Stack<M, A, A2> as svc::Stack<T>>::Error;
    type Stack = Stack<M, A, A2>;

    fn bind(&self, inner: M) -> Self::Stack {
        Stack {
            inner,
            percentile: self.percentile,
            period: self.period,
            _p: PhantomData,
        }
    }
}

impl<A, A2> Clone for Layer<A, A2> {
    fn clone(&self) -> Self {
        Layer {
            percentile: self.percentile,
            period: self.period,
            _p: PhantomData,
        }
    }
}
