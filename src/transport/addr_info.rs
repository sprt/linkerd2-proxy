use std::env;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpStream;

#[cfg(target_os = "windows")]
extern crate winapi;
extern crate ws2_32;

pub trait AddrInfo: Debug {
    fn local_addr(&self) -> Result<SocketAddr, io::Error>;
    fn get_original_dst(&self) -> Option<SocketAddr>;
}

impl<T: AddrInfo + ?Sized> AddrInfo for Box<T> {
    fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.as_ref().local_addr()
    }

    fn get_original_dst(&self) -> Option<SocketAddr> {
        self.as_ref().get_original_dst()
    }
}

impl AddrInfo for TcpStream {
    fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        TcpStream::local_addr(&self)
    }

    #[cfg(target_os = "linux")]
    fn get_original_dst(&self) -> Option<SocketAddr> {
        use std::os::unix::io::AsRawFd;

        let fd = self.as_raw_fd();
        let r = unsafe { linux::so_original_dst(fd) };
        r.ok()
    }

    #[cfg(target_os = "windows")]
    fn get_original_dst(&self) -> Option<SocketAddr> {
        match env::var_os("X_LINKERD2_SO_ORIGINAL_DST") {
            Some(val) => {
                let addr: SocketAddr = val
                    .into_string()
                    .unwrap()
                    .parse()
                    .expect("Unable to parse X_LINKERD2_SO_ORIGINAL_DST");
                Some(addr)
            },
            None => {
                debug!("X_LINKERD2_SO_ORIGINAL_DST not set");
                None
            }
        }
        
        // use std::os::windows::io::AsRawSocket;

        // let fd = self.as_raw_socket();
        // let r = unsafe { windows::so_original_dst(fd) };
        // r.ok()
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    fn get_original_dst(&self) -> Option<SocketAddr> {
        debug!("no support for SO_ORIGINAL_DST");
        None
    }
}

/// A generic way to get the original destination address of a socket.
///
/// This is especially useful to allow tests to provide a mock implementation.
pub trait GetOriginalDst {
    fn get_original_dst(&self, socket: &AddrInfo) -> Option<SocketAddr>;
}

#[derive(Copy, Clone, Debug)]
pub struct SoOriginalDst;

impl GetOriginalDst for SoOriginalDst {
    fn get_original_dst(&self, sock: &AddrInfo) -> Option<SocketAddr> {
        trace!("get_original_dst {:?}", sock);
        sock.get_original_dst()
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use libc;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
    use std::os::unix::io::RawFd;
    use std::{io, mem};

    pub unsafe fn so_original_dst(fd: RawFd) -> io::Result<SocketAddr> {
        let mut sockaddr: libc::sockaddr_storage = mem::zeroed();
        let mut socklen: libc::socklen_t = mem::size_of::<libc::sockaddr_storage>() as u32;

        let ret = libc::getsockopt(
            fd,
            libc::SOL_IP,
            libc::SO_ORIGINAL_DST,
            &mut sockaddr as *mut _ as *mut _,
            &mut socklen as *mut _ as *mut _,
        );
        if ret != 0 {
            let e = io::Error::last_os_error();
            warn!("failed to read SO_ORIGINAL_DST: {:?}", e);
            return Err(e);
        }

        mk_addr(&sockaddr, socklen)
    }

    // Borrowed with love from net2-rs
    // https://github.com/rust-lang-nursery/net2-rs/blob/1b4cb4fb05fbad750b271f38221eab583b666e5e/src/socket.rs#L103
    fn mk_addr(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> io::Result<SocketAddr> {
        match storage.ss_family as libc::c_int {
            libc::AF_INET => {
                assert!(len as usize >= mem::size_of::<libc::sockaddr_in>());

                let sa = {
                    let sa = storage as *const _ as *const libc::sockaddr_in;
                    unsafe { *sa }
                };

                let bits = ntoh32(sa.sin_addr.s_addr);
                let ip = Ipv4Addr::new(
                    (bits >> 24) as u8,
                    (bits >> 16) as u8,
                    (bits >> 8) as u8,
                    bits as u8,
                );
                let port = sa.sin_port;
                Ok(SocketAddr::V4(SocketAddrV4::new(ip, ntoh16(port))))
            }
            libc::AF_INET6 => {
                assert!(len as usize >= mem::size_of::<libc::sockaddr_in6>());

                let sa = {
                    let sa = storage as *const _ as *const libc::sockaddr_in6;
                    unsafe { *sa }
                };

                let arr = sa.sin6_addr.s6_addr;
                let ip = Ipv6Addr::new(
                    (arr[0] as u16) << 8 | (arr[1] as u16),
                    (arr[2] as u16) << 8 | (arr[3] as u16),
                    (arr[4] as u16) << 8 | (arr[5] as u16),
                    (arr[6] as u16) << 8 | (arr[7] as u16),
                    (arr[8] as u16) << 8 | (arr[9] as u16),
                    (arr[10] as u16) << 8 | (arr[11] as u16),
                    (arr[12] as u16) << 8 | (arr[13] as u16),
                    (arr[14] as u16) << 8 | (arr[15] as u16),
                );

                let port = sa.sin6_port;
                let flowinfo = sa.sin6_flowinfo;
                let scope_id = sa.sin6_scope_id;
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    ntoh16(port),
                    flowinfo,
                    scope_id,
                )))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid argument",
            )),
        }
    }

    fn ntoh16(i: u16) -> u16 {
        <u16>::from_be(i)
    }

    fn ntoh32(i: u32) -> u32 {
        <u32>::from_be(i)
    }
}

// #[cfg(target_os = "windows")]
// mod windows {
//     // use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
//     use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
//     use std::os::windows::io::RawSocket;
//     use std::{io, mem};
//     // use transport::addr_info::winapi;
//     use transport::addr_info::winapi::shared::ws2def;
//     use transport::addr_info::ws2_32;
//     use transport::addr_info::winapi::shared::minwindef::DWORD;

//     pub unsafe fn so_original_dst(fd: RawSocket) -> io::Result<SocketAddr> {
//         let mut value: DWORD = 0;
//         let mut value_len: ::std::os::raw::c_int = mem::size_of_val(&value) as _;

//         let ret = ws2_32::getsockopt(
//             fd,
//             ws2def::IPPROTO_IP,
//             0x300f,
//             // &mut value as *mut DWORD as _,
//             &mut value as *mut _ as *mut _,
//             &mut value_len,
//         );
//         if ret != 0 {
//             let e = io::Error::from_raw_os_error(ws2_32::WSAGetLastError());
//             warn!("failed to read SO_ORIGINAL_DST: {:?}", e);
//             return Err(e);
//         }

//         mk_addr(&value, value_len)
//     }

//     // Borrowed with love from net2-rs
//     // https://github.com/rust-lang-nursery/net2-rs/blob/1b4cb4fb05fbad750b271f38221eab583b666e5e/src/socket.rs#L103
//     fn mk_addr(storage: &ws2def::SOCKADDR_STORAGE, len: ::std::os::raw::c_int) -> io::Result<SocketAddr> {
//         match storage.ss_family as ::std::os::raw::c_int {
//             ws2def::AF_INET => {
//                 assert!(len as usize >= mem::size_of::<ws2def::SOCKADDR_IN>());

//                 let sa = {
//                     let sa = storage as *const _ as *const ws2def::SOCKADDR_IN;
//                     unsafe { *sa }
//                 };

//                 let bits = ntoh32(sa.sin_addr.S_un.s_addr);
//                 let ip = Ipv4Addr::new(
//                     (bits >> 24) as u8,
//                     (bits >> 16) as u8,
//                     (bits >> 8) as u8,
//                     bits as u8,
//                 );
//                 let port = sa.sin_port;
//                 Ok(SocketAddr::V4(SocketAddrV4::new(ip, ntoh16(port))))
//             }
//             // ws2def::AF_INET6 => {
//             //     assert!(len as usize >= mem::size_of::<winapi::shared::ws2ipdef::sockaddr_in6>());

//             //     let sa = {
//             //         let sa = storage as *const _ as *const winapi::ws2ipdef::sockaddr_in6;
//             //         unsafe { *sa }
//             //     };

//             //     let arr = sa.sin6_addr.s6_addr;
//             //     let ip = Ipv6Addr::new(
//             //         (arr[0] as u16) << 8 | (arr[1] as u16),
//             //         (arr[2] as u16) << 8 | (arr[3] as u16),
//             //         (arr[4] as u16) << 8 | (arr[5] as u16),
//             //         (arr[6] as u16) << 8 | (arr[7] as u16),
//             //         (arr[8] as u16) << 8 | (arr[9] as u16),
//             //         (arr[10] as u16) << 8 | (arr[11] as u16),
//             //         (arr[12] as u16) << 8 | (arr[13] as u16),
//             //         (arr[14] as u16) << 8 | (arr[15] as u16),
//             //     );

//             //     let port = sa.sin6_port;
//             //     let flowinfo = sa.sin6_flowinfo;
//             //     let scope_id = sa.sin6_scope_id;
//             //     Ok(SocketAddr::V6(SocketAddrV6::new(
//             //         ip,
//             //         ntoh16(port),
//             //         flowinfo,
//             //         scope_id,
//             //     )))
//             // }
//             _ => Err(io::Error::new(
//                 io::ErrorKind::InvalidInput,
//                 "invalid argument",
//             )),
//         }
//     }

//     fn ntoh16(i: u16) -> u16 {
//         <u16>::from_be(i)
//     }

//     fn ntoh32(i: u32) -> u32 {
//         <u32>::from_be(i)
//     }
// }
