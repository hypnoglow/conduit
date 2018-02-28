use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut};
use futures::{future, Async, Future, Poll};
use tokio_connect::Connect;
use tokio_core::reactor::Handle;
use tokio_io::{AsyncRead, AsyncWrite};
//use tokio_io::io::copy;

use conduit_proxy_controller_grpc::common;
use ctx::transport::{Client as ClientCtx, Server as ServerCtx};
use telemetry::Sensors;
use timeout::Timeout;
use transport;

/// TCP Server Proxy
#[derive(Debug, Clone)]
pub struct Proxy {
    connect_timeout: Duration,
    executor: Handle,
    sensors: Sensors,
}

impl Proxy {
    /// Create a new TCP `Proxy`.
    pub fn new(connect_timeout: Duration, sensors: Sensors, executor: &Handle) -> Self {
        Self {
            connect_timeout,
            executor: executor.clone(),
            sensors,
        }
    }

    /// Serve a TCP connection, trying to forward it to its destination.
    pub fn serve<T>(&self, tcp_in: T, srv_ctx: Arc<ServerCtx>) -> Box<Future<Item=(), Error=()>>
    where
        T: AsyncRead + AsyncWrite + 'static,
    {
        let orig_dst = srv_ctx.orig_dst_if_not_local();

        // For TCP, we really have no extra information other than the
        // SO_ORIGINAL_DST socket option. If that isn't set, the only thing
        // to do is to drop this connection.
        let orig_dst = if let Some(orig_dst) = orig_dst {
            debug!(
                "tcp accepted, forwarding ({}) to {}",
                srv_ctx.remote,
                orig_dst,
            );
            orig_dst
        } else {
            debug!(
                "tcp accepted, no SO_ORIGINAL_DST to forward: remote={}",
                srv_ctx.remote,
            );
            return Box::new(future::ok(()));
        };

        let client_ctx = ClientCtx::new(
            &srv_ctx.proxy,
            &orig_dst,
            common::Protocol::Tcp,
        );
        let c = Timeout::new(
            transport::Connect::new(orig_dst, &self.executor),
            self.connect_timeout,
            &self.executor,
        );
        let connect = self.sensors.connect(c, &client_ctx);

        let fut = connect.connect()
            .map_err(|e| debug!("tcp connect error: {:?}", e))
            .and_then(move |tcp_out| {
                Duplex::new(tcp_in, tcp_out)
                    .map_err(|e| debug!("tcp error: {}", e))
            });
        Box::new(fut)
    }
}

/// A future piping data bi-directionally to In and Out.
struct Duplex<In, Out> {
    in_buf: CopyBuf,
    in_eof: bool,
    in_io: In,
    in_shutdown: bool,
    out_buf: CopyBuf,
    out_eof: bool,
    out_io: Out,
    out_shutdown: bool,
}

/// A buffer used to copy bytes from one IO to another.
///
/// Keeps read and write positions.
struct CopyBuf {
    // In linkerd-tcp, a shared buffer is used to start, and an allocation is
    // only made if NotReady is found trying to flush the buffer. We could
    // consider making the same optimization here.
    buf: Box<[u8]>,
    read_pos: usize,
    write_pos: usize,
}

impl<In, Out> Duplex<In, Out>
where
    In: AsyncRead + AsyncWrite,
    Out: AsyncRead + AsyncWrite,
{
    fn new(in_io: In, out_io: Out) -> Self {
        Duplex {
            in_buf: CopyBuf::new(),
            in_eof: false,
            in_io,
            in_shutdown: false,
            out_buf: CopyBuf::new(),
            out_eof: false,
            out_io,
            out_shutdown: false,
        }
    }

    /// Returns true if the Duplex is completely done.
    ///
    /// The future may finish earlier if one side has an error.
    fn is_done(&self) -> bool {
        self.in_eof
            && !self.in_buf.has_remaining()
            && self.out_eof
            && !self.out_buf.has_remaining()
    }
}

impl<In, Out> Future for Duplex<In, Out>
where
    In: AsyncRead + AsyncWrite,
    Out: AsyncRead + AsyncWrite,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // this is macro because we don't want to actually split the IO, as
        // that requires a futures 'TryLock'. Instead, so that we can skip
        // that lock, the Duplex holds the original IO, and this macro
        // prevents duplication of code.
        macro_rules! pipe_side {
            ($buf:ident, $eof:ident, $shutdown:ident, $read:ident, $write:ident) => ({
                let mut __ready = !self.$shutdown;
                while __ready {
                    if !self.$buf.has_remaining() && !self.$eof {
                        self.$buf.reset();
                        match self.$read.read_buf(&mut self.$buf)? {
                            Async::Ready(0) => {
                                self.$eof = true;
                            },
                            Async::Ready(_) => {},
                            Async::NotReady => {
                                __ready = false;
                            },
                        }
                    }

                    while self.$buf.has_remaining() {
                        match self.$write.write_buf(&mut self.$buf)? {
                            Async::Ready(0) => {
                                return Err(write_zero());
                            },
                            Async::Ready(_) => {
                            },
                            Async::NotReady => {
                                __ready = false;
                                break;
                            },
                        }
                    }

                    if self.$eof && !self.$shutdown && !self.$buf.has_remaining() {
                        try_ready!(self.$write.shutdown());
                        self.$shutdown = true;
                        __ready = false;
                    }
                }
            })
        }

        pipe_side!(in_buf, in_eof, in_shutdown, in_io, out_io);
        pipe_side!(out_buf, out_eof, out_shutdown, out_io, in_io);


        if self.is_done() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

fn write_zero() -> io::Error {
    io::Error::new(io::ErrorKind::WriteZero, "write zero bytes")
}

impl CopyBuf {
    fn new() -> Self {
        CopyBuf {
            buf: Box::new([0; 4096]),
            read_pos: 0,
            write_pos: 0,
        }
    }

    fn reset(&mut self) {
        debug_assert_eq!(self.read_pos, self.write_pos);
        self.read_pos = 0;
        self.write_pos = 0;
    }
}

impl Buf for CopyBuf {
    fn remaining(&self) -> usize {
        self.write_pos - self.read_pos
    }

    fn bytes(&self) -> &[u8] {
        &self.buf[self.read_pos..self.write_pos]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(self.write_pos >= self.read_pos + cnt);
        self.read_pos += cnt;
    }
}

impl BufMut for CopyBuf {
    fn remaining_mut(&self) -> usize {
        self.buf.len() - self.write_pos
    }

    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.write_pos..]
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(self.buf.len() >= self.write_pos + cnt);
        self.write_pos += cnt;
    }
}
