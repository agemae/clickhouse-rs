use std::{
    collections::VecDeque,
    io::{self, Cursor},
    pin::Pin,
    ptr,
    sync::{
        self,
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{self, Poll},
};

use chrono_tz::Tz;
use log::trace;

use pin_project::pin_project;

use crate::{
    binary::Parser,
    errors::{ConnectionError, DriverError, Error, Result, ServerError},
    io::{read_to_end::read_to_end, Stream as InnerStream},
    types::{Block, Cmd, Packet},
};
use futures_core::Stream;
use futures_util::{future, StreamExt};

pub(crate) struct TransportInfo {
    pub(crate) timezone: Option<Tz>,
    pub(crate) revision: u64,
    pub(crate) compress: bool,
}

/// What a command that a server `Exception` can end returns: the exception comes back with
/// the transport, since the connection is idle and reusable afterwards.
pub(crate) type ServerReply<T> = std::result::Result<T, (ClickhouseTransport, ServerError)>;

/// Line transport
#[pin_project(project = ClickhouseTransportProj)]
pub(crate) struct ClickhouseTransport {
    // Inner socket
    #[pin]
    inner: InnerStream,
    // Set to true when inner.read returns Ok(0);
    done: bool,
    // Buffered read data
    rd: Vec<u8>,
    // Whether the buffer is known to be incomplete
    buf_is_incomplete: bool,
    // Current buffer to write to the socket
    wr: io::Cursor<Vec<u8>>,
    // Queued commands
    cmds: VecDeque<Cmd>,
    // Server time zone
    // timezone: Option<Tz>,
    // revision: u64,
    // compress: bool,
    info: TransportInfo,
    // Whether there are unread packets
    pub(crate) inconsistent: bool,
}

enum PacketStreamState {
    Ask,
    Receive,
    Yield(Box<Option<Packet<ClickhouseTransport>>>),
    Done,
}

pub(crate) struct PacketStream {
    inner: Option<ClickhouseTransport>,
    state: PacketStreamState,
    read_block: bool,
}

impl ClickhouseTransport {
    pub fn new(inner: InnerStream, compress: bool) -> Self {
        ClickhouseTransport {
            inner,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr: io::Cursor::new(vec![]),
            cmds: VecDeque::new(),
            info: TransportInfo {
                timezone: None,
                revision: 0,
                compress,
            },
            inconsistent: false,
        }
    }

    /// Drains what an abandoned query (a result stream dropped before its end) left on the
    /// connection in `slot`, so the next command starts in sync with the server.
    ///
    /// Cancel-safe: if this future is dropped part-way (a timeout, a `select!`), the transport
    /// goes back into `slot` still marked `inconsistent`, and the next call drains the rest.
    /// A read error or a closed socket leaves `slot` empty.
    pub(crate) async fn clear_in(slot: &mut Option<ClickhouseTransport>) -> Result<()> {
        match slot {
            None => return Err(Error::Connection(ConnectionError::Broken)),
            Some(transport) if !transport.inconsistent => return Ok(()),
            Some(_) => {}
        }
        let transport = slot.take().expect("slot checked above");
        let mut drain = Drain {
            stream: transport.call(Cmd::Cancel),
            slot,
        };

        while let Some(packet) = drain.stream.next().await {
            match packet {
                // `EndOfStream`, or an `Exception`, is the abandoned query's last packet.
                Ok(Packet::Eof(mut transport))
                | Ok(Packet::Exception(mut transport, _))
                | Ok(Packet::Pong(mut transport)) => {
                    transport.inconsistent = false;
                    *drain.slot = Some(transport);
                    return Ok(());
                }
                // Each leftover block is decoded in full; yield so a long drain doesn't hold
                // the executor and a timeout around the caller can fire.
                Ok(_) => yield_now().await,
                Err(e) => {
                    drain.stream.take_transport();
                    return Err(Error::Io(e));
                }
            }
        }

        drain.stream.take_transport();
        Err(Error::Connection(ConnectionError::Broken))
    }
}

/// Puts the transport back into the handle when a drain is dropped part-way.
struct Drain<'a> {
    stream: PacketStream,
    slot: &'a mut Option<ClickhouseTransport>,
}

impl Drop for Drain<'_> {
    fn drop(&mut self) {
        if let Some(mut transport) = self.stream.take_transport() {
            transport.inconsistent = true;
            *self.slot = Some(transport);
        }
    }
}

/// Returns `Pending` once, after waking the task, so other tasks get to run.
async fn yield_now() {
    let mut yielded = false;
    future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

impl<'p> ClickhouseTransportProj<'p> {
    fn try_parse_msg(&mut self) -> Poll<Option<io::Result<Packet<()>>>> {
        let pos;
        let ret = {
            let mut cursor = Cursor::new(&self.rd);
            let res = {
                let mut parser = Parser::new(&mut cursor, self.info);
                parser.parse_packet(self.info.revision)
            };
            pos = cursor.position() as usize;

            if let Ok(Packet::Hello(_, ref packet)) = res {
                self.info.timezone = Some(packet.timezone);
                self.info.revision = packet.revision;
            }

            match res {
                Ok(val) => Poll::Ready(Some(Ok(val))),
                Err(e) => {
                    if e.is_would_block() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(Err(e.into())))
                    }
                }
            }
        };

        match ret {
            Poll::Pending => (),
            _ => {
                // Data is consumed
                let new_len = self.rd.len() - pos;
                unsafe {
                    ptr::copy(self.rd.as_ptr().add(pos), self.rd.as_mut_ptr(), new_len);
                    self.rd.set_len(new_len);
                }
            }
        }

        ret
    }
}

impl ClickhouseTransport {
    fn wr_is_empty(&self) -> bool {
        self.wr_remaining() == 0
    }

    fn wr_remaining(&self) -> usize {
        self.wr.get_ref().len() - self.wr_pos()
    }

    fn wr_pos(&self) -> usize {
        self.wr.position() as usize
    }

    fn wr_flush(&mut self, cx: &mut task::Context) -> io::Result<bool> {
        // Making the borrow checker happy
        let res = {
            let buf = {
                let pos = self.wr.position() as usize;
                let buf = &self.wr.get_ref()[pos..];

                trace!("writing; remaining={:?}", buf);
                buf
            };

            Pin::new(&mut self.inner).poll_write(cx, buf)
        };

        match res {
            Poll::Ready(Ok(mut n)) => {
                n += self.wr.position() as usize;
                self.wr.set_position(n as u64);
                Ok(true)
            }
            Poll::Ready(Err(e)) => {
                trace!("transport flush error; err={:?}", e);
                Err(e)
            }
            Poll::Pending => Ok(false),
        }
    }

    fn send(&mut self, cx: &mut task::Context) -> Poll<Result<()>> {
        loop {
            if self.wr_is_empty() {
                match self.cmds.pop_front() {
                    None => return Poll::Ready(Ok(())),
                    Some(cmd) => {
                        let bytes = cmd.get_packed_command()?;
                        self.wr = Cursor::new(bytes)
                    }
                }
            }

            // Try to write the remaining buffer
            if !self.wr_flush(cx)? {
                return Poll::Pending;
            }
        }
    }
}

impl Stream for ClickhouseTransport {
    type Item = io::Result<Packet<()>>;

    /// Read a message from the `Transport`
    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // Check whether our currently buffered data is enough for a packet
        // before reading any more data. This prevents the buffer from growing
        // indefinitely when the sender is faster than we can consume the data
        if !*this.buf_is_incomplete && !this.rd.is_empty() {
            if let Poll::Ready(ret) = this.try_parse_msg()? {
                return Poll::Ready(ret.map(Ok));
            }
        }

        // Fill the buffer!
        while !*this.done {
            match read_to_end(this.inner.as_mut(), cx, this.rd) {
                Poll::Ready(Ok(0)) => {
                    *this.done = true;
                    break;
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => break,
            }
        }

        if *this.done {
            return Poll::Ready(None);
        }

        // Try to parse the new data!
        let ret = this.try_parse_msg();

        *this.buf_is_incomplete = matches!(ret, Poll::Pending);

        ret
    }
}

impl PacketStream {
    /// Reads up to the first data block or the end of stream. A server `Exception` also ends
    /// the exchange: it comes back as the inner `Err`, together with the transport, which is
    /// idle again and can be reused.
    pub(crate) async fn read_block(
        mut self,
    ) -> Result<ServerReply<(ClickhouseTransport, Option<Block>)>> {
        self.read_block = true;

        let mut h = None;
        let mut b = None;
        while let Some(package) = self.next().await {
            match package {
                Ok(Packet::Eof(inner)) => h = Some(inner),
                Ok(Packet::Block(block)) => b = Some(block),
                Ok(Packet::Exception(inner, e)) => return Ok(Err((inner, e))),
                // The server reports progress and an execution profile while it runs an INSERT.
                Ok(Packet::TableColumns(_))
                | Ok(Packet::Progress(_))
                | Ok(Packet::ProfileInfo(_)) => (),
                Err(e) => return Err(Error::Io(e)),
                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }

        let h = h.ok_or(Error::Connection(ConnectionError::Broken))?;
        Ok(Ok((h, b)))
    }

    pub(crate) fn take_transport(&mut self) -> Option<ClickhouseTransport> {
        self.inner.take()
    }
}

impl Stream for PacketStream {
    type Item = io::Result<Packet<ClickhouseTransport>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        loop {
            self.state = match self.state {
                PacketStreamState::Ask => match self.inner {
                    None => PacketStreamState::Done,
                    Some(ref mut inner) => {
                        match inner.send(cx) {
                            Poll::Ready(Ok(t)) => t,
                            Poll::Ready(Err(e)) => {
                                if e.is_would_block() {
                                    return Poll::Pending;
                                }

                                return Poll::Ready(Some(Err(e.into())));
                            }
                            Poll::Pending => return Poll::Pending,
                        };
                        PacketStreamState::Receive
                    }
                },
                PacketStreamState::Receive => {
                    let ret = match self.inner {
                        None => None,
                        Some(ref mut inner) => match Pin::new(inner).poll_next(cx) {
                            Poll::Ready(Some(Ok(r))) => Some(r),
                            Poll::Ready(Some(Err(e))) => {
                                if e.kind() == io::ErrorKind::WouldBlock {
                                    return Poll::Pending;
                                }

                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Ready(None) => return Poll::Ready(None),
                            Poll::Pending => return Poll::Pending,
                        },
                    };

                    match ret {
                        None => PacketStreamState::Done,
                        Some(packet) => {
                            let result = packet.bind(&mut self.inner);
                            PacketStreamState::Yield(Box::new(Some(result)))
                        }
                    }
                }
                PacketStreamState::Yield(_) => PacketStreamState::Receive,
                PacketStreamState::Done => {
                    return match self.inner.take() {
                        Some(inner) => Poll::Ready(Some(Ok(Packet::Eof(inner)))),
                        _ => Poll::Ready(None),
                    };
                }
            };

            let package = match self.state {
                PacketStreamState::Yield(ref mut packet) => packet.take(),
                _ => None,
            };

            if self.read_block && is_block(&package) {
                self.state = PacketStreamState::Done;
            }

            if let Some(pkg) = package {
                return Poll::Ready(Some(Ok(pkg)));
            }
        }
    }
}

impl ClickhouseTransport {
    pub(crate) fn call(mut self, req: Cmd) -> PacketStream {
        self.cmds.push_back(req);
        PacketStream {
            inner: Some(self),
            state: PacketStreamState::Ask,
            read_block: false,
        }
    }
}

fn is_block<T>(packet: &Option<Packet<T>>) -> bool {
    matches!(packet, Some(Packet::Block(_)))
}
