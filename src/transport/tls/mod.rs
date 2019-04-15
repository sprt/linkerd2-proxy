// These crates are only used within the `tls` module.
extern crate rustls;
extern crate tokio_rustls;
extern crate untrusted;
extern crate webpki;

use self::tokio_rustls::{Accept, TlsAcceptor as Acceptor, TlsConnector as Connector};
use std::fmt;

use identity::{self, Name};

pub mod client;
mod conditional_accept;
mod connection;
mod io;
pub mod listen;

use self::io::TlsIo;

pub use self::connection::Connection;
pub use self::listen::Listen;
pub use self::rustls::TLSError as Error;

// ----- Remove -----

pub type Status = Conditional<()>;

pub trait HasStatus {
    fn tls_status(&self) -> Status;
}

impl<T: HasPeerIdentity> HasStatus for T {
    fn tls_status(&self) -> Status {
        self.peer_identity().map(|_| ())
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ::Conditional::Some(()) => write!(f, "true"),
            ::Conditional::None(r) => fmt::Display::fmt(&r, f),
        }
    }
}

// -----

/// Describes whether or not a connection was secured with TLS and, if it was
/// not, the reason why.
// pub type Conditional<T> = ::Conditional<T, ReasonForNoIdentity>;
pub type PeerIdentity = Conditional<identity::Name>;

pub trait HasPeerIdentity {
    fn peer_identity(&self) -> PeerIdentity;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasonForNoIdentity {
    /// Identity is administratively disabled.
    Disabled,

    /// The remote peer does not have a known identity name.
    NoPeerName(ReasonForNoPeerName),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasonForNoPeerName {
    /// The connection is a non-HTTP connection so we don't know anything
    /// about the destination besides its address.
    NotHttp,

    /// The connection is for HTTP but the HTTP request doesn't have an
    /// authority so we can't extract the identity from it.
    NoAuthorityInHttpRequest,

    /// The destination service didn't give us the identity, which is its way
    /// of telling us that we shouldn't do TLS for this endpoint.
    NotProvidedByServiceDiscovery,

    /// No TLS is wanted because the connection is a loopback connection which
    /// doesn't need or support TLS.
    Loopback,

    // Identity was not provided by the remote peer.
    NotProvidedByRemote,
}

impl From<ReasonForNoPeerName> for ReasonForNoIdentity {
    fn from(r: ReasonForNoPeerName) -> Self {
        ReasonForNoIdentity::NoPeerName(r)
    }
}

impl fmt::Display for ReasonForNoIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReasonForNoIdentity::Disabled => write!(f, "disabled"),
            ReasonForNoIdentity::NoPeerName(n) => write!(f, "{}", n),
        }
    }
}

impl fmt::Display for ReasonForNoPeerName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReasonForNoPeerName::Loopback => write!(f, "loopback"),
            ReasonForNoPeerName::NoAuthorityInHttpRequest => {
                write!(f, "no_authority_in_http_request")
            }
            ReasonForNoPeerName::NotHttp => write!(f, "not_http"),
            ReasonForNoPeerName::NotProvidedByRemote => write!(f, "not_provided_by_remote"),
            ReasonForNoPeerName::NotProvidedByServiceDiscovery => {
                write!(f, "not_provided_by_service_discovery")
            }
        }
    }
}

// ===== changes =====
// pub type Conditional<T> = ::Conditional<T, ReasonForNoTls>;
pub type Conditional<T> = ::Conditional<T, ReasonForNoTls>;
pub type Identity<T> = ::Conditional<T, ReasonForNoClientIdentity>;

pub trait HasIdentity {
    fn identity(&self) -> Identity<identity::Name>;
}

pub trait HasConditional {
    fn tls(&self) -> Conditional<TlsState>;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasonForNoTls {
    /// TLS is administratively disabled.
    Disabled,

    /// The client does not have a known identity.
    NoClientIdentity(ReasonForNoClientIdentity),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasonForNoClientIdentity {
    /// No TLS is wanted because the connection is a loopback connection which
    /// doesn't need or support TLS.
    Loopback,

    /// The connection is for HTTP but the HTTP request doesn't have an
    /// authority so we can't extract the identity from it.
    NoAuthorityInHttpRequest,

    /// The connection is a non-HTTP connection so we don't know anything
    /// about the destination besides its address.
    NotHttp,

    // Identity was not provided by the client.
    NotProvidedByClient,

    /// The destination service didn't give us the identity, which is its way
    /// of telling us that we shouldn't do TLS for this endpoint.
    NotProvidedByServiceDiscovery,
}

impl From<ReasonForNoClientIdentity> for ReasonForNoTls {
    fn from(r: ReasonForNoClientIdentity) -> Self {
        ReasonForNoTls::NoClientIdentity(r)
    }
}

#[derive(Clone, Debug)]
pub struct TlsState {
    server_identity: Name,
    client_identity: Identity<Name>,
}

impl TlsState {
    pub fn client_identity(&self) -> Identity<Name> {
        self.client_identity
    }
}

// ===== impl ReasonForNoTls =====

impl fmt::Display for ReasonForNoTls {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReasonForNoTls::Disabled => write!(f, "disabled"),
            ReasonForNoTls::NoClientIdentity(n) => write!(f, "{}", n),
        }
    }
}

// ===== impl ReasonForNoClientIdentity =====

impl fmt::Display for ReasonForNoClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReasonForNoClientIdentity::Loopback => write!(f, "loopback"),
            ReasonForNoClientIdentity::NoAuthorityInHttpRequest => {
                write!(f, "no_authority_in_http_request")
            }
            ReasonForNoClientIdentity::NotHttp => write!(f, "not_http"),
            ReasonForNoClientIdentity::NotProvidedByClient => write!(f, "not_provided_by_client"),
            ReasonForNoClientIdentity::NotProvidedByServiceDiscovery => {
                write!(f, "not_provided_by_service_discovery")
            }
        }
    }
}
