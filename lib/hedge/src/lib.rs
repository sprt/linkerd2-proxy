#[macro_use]
extern crate futures;
extern crate linkerd2_metrics;
extern crate tokio_timer;
extern crate tower_service;

use futures::{Async, Future, Poll};
use linkerd2_metrics::histogram::Histogram;
use linkerd2_metrics::latency;
use tokio_timer::{clock, Delay};
use tower_service::Service;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod rotating;

use rotating::Rotating;

/// A "retry policy" to classify if a request should be pre-emptively retried.
pub trait Policy<Request>: Sized {
    fn can_retry(&self, req: &Request) -> bool;
    fn clone_request(&self, req: &Request) -> Option<Request>;
}

/// A middleware pre-emptively retries requests which have been outstanding for
/// longer than a given latency percentile.  If either of the original future
/// or the retry future completes, that value is used.
#[derive(Clone)]
pub struct Hedge<P, S> {
    policy: P,
    service: S,
    latency_percentile: f32,
    // A rotating histogram is used to track response latency.
    pub latency_histogram: Arc<Mutex<Rotating<Histogram<latency::Ms>>>>,
}

pub struct ResponseFuture<P, S, Request>
where
    P: Policy<Request>,
    S: Service<Request>,
{
    // If the request was clonable, a clone is stored.
    request: Option<Request>,
    // The time of the original call to the inner service.  Used to calculate
    // response latency.
    start: Instant,
    hedge: Hedge<P, S>,
    orig_fut: S::Future,
    hedge_fut: Option<S::Future>,
    // A future representing when to start the hedge request.
    delay: Option<Delay>,
}

impl<P, S> Hedge<P, S> {
    pub fn new<Request>(
        policy: P,
        service: S,
        latency_percentile: f32,
        rotation_period: Duration,
    ) -> Self
    where
        P: Policy<Request> + Clone,
        S: Service<Request>,
    {
        let new: fn() -> Histogram<latency::Ms> = || Histogram::new(latency::BOUNDS);
        let latency_histogram = Arc::new(Mutex::new(Rotating::new(rotation_period, new)));
        Hedge {
            policy,
            service,
            latency_percentile,
            latency_histogram,
        }
    }
}

impl<P, S, Request> Service<Request> for Hedge<P, S>
where
    P: Policy<Request> + Clone,
    S: Service<Request> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<P, S, Request>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.service.poll_ready()
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let cloned = self.policy.clone_request(&request);
        let orig_fut = self.service.call(request);

        let start = clock::now();
        // Find the nth percentile latency from the read side of the histogram.
        // Requests which take longer than this will be pre-emptively retried.
        let mut histo = self.latency_histogram.lock().unwrap();
        // TODO: Consider adding a minimum delay for hedge requests (perhaps as
        // a factor of the p50 latency).
        let delay = histo
            .read()
            // We will only issue a hedge request if there are sufficiently many
            // data points in the histogram to give us confidence about the
            // distribution.
            .percentile(self.latency_percentile, 10)
            .map(|hedge_timeout| Delay::new(start + Duration::from_millis(hedge_timeout)));

        ResponseFuture {
            request: cloned,
            start,
            hedge: self.clone(),
            orig_fut,
            hedge_fut: None,
            delay,
        }
    }
}

impl<P, S, Request> ResponseFuture<P, S, Request>
where
    P: Policy<Request>,
    S: Service<Request>,
{
    /// Record the latency of a completed request in the latency histogram.
    fn record(&mut self) {
        let duration = clock::now() - self.start;
        let mut histo = self.hedge.latency_histogram.lock().unwrap();
        histo.write().add(duration);
    }
}

impl<P, S, Request> Future for ResponseFuture<P, S, Request>
where
    P: Policy<Request> + Clone,
    S: Service<Request> + Clone,
{
    type Item = S::Response;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // If the original future is complete, return its result.
            match self.orig_fut.poll() {
                Ok(Async::Ready(rsp)) => {
                    self.record();
                    return Ok(Async::Ready(rsp));
                }
                Ok(Async::NotReady) => {}
                Err(e) => {
                    self.record();
                    return Err(e);
                }
            }

            if let Some(ref mut hedge_fut) = self.hedge_fut {
                // If the hedge future exists, return its result.
                return hedge_fut.poll();
            }
            // Original future is pending, but hedge hasn't started.  Check
            // the delay.
            let delay = match self.delay.as_mut() {
                Some(d) => d,
                // No delay, can't retry.
                None => return Ok(Async::NotReady),
            };
            match delay.poll() {
                Ok(Async::Ready(_)) => {
                    try_ready!(self.hedge.poll_ready());
                    if let Some(req) = self.request.take() {
                        if self.hedge.policy.can_retry(&req) {
                            // Start the hedge request.
                            self.request = self.hedge.policy.clone_request(&req);
                            self.hedge_fut = Some(self.hedge.service.call(req));
                        } else {
                            // Policy says we can't retry.
                            // Put the taken request back.
                            self.request = Some(req);
                            return Ok(Async::NotReady);
                        }
                    } else {
                        // No cloned request, can't retry.
                        return Ok(Async::NotReady);
                    }
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady), // Not time to retry yet.
                Err(_) => {
                    // TODO: log timer error
                    // Timer error, don't retry.
                    return Ok(Async::NotReady);
                },
            }
        }
    }
}

impl<V> rotating::Clear for Histogram<V>
where
    V: Into<u64>,
{
    fn clear(&mut self) {
        self.clear_buckets();
    }
}
