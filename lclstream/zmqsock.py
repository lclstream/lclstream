from typing import TypeVar, Optional, Union
from collections.abc import Iterator, Callable
import io
import logging
_logger = logging.getLogger(__name__)

import stream
import h5py # type: ignore[import-untyped]
from zmq import ZMQError
import zmq
import zmq.utils.monitor as zmq_monitor

T = TypeVar('T')
def load_h5(buf: bytes, reader: Callable[[h5py.File],T]) -> Optional[T]:
    """ Simple function to read an hdf5 file from
    its serialized bytes representation.

    Returns the result of calling `reader(h5file)`
    or None on error.
    """
    try:
        with io.BytesIO(buf) as f:
            with h5py.File(f, 'r') as h:
                return reader(h)
    except (IOError, OSError):
        pass
    return None

@stream.stream
def pusher(gen: Iterator[bytes], addr: str, ndial: int
          ) -> Iterator[int]:
    # transform messages sent into sizes sent
    assert ndial >= 0

    ctxt = zmq.Context.instance()
    with ctxt.socket(zmq.PUSH) as socket:
        # Don't queue messages until a receiver connects.
        socket.setsockopt(zmq.IMMEDIATE, 1)

        try:
            if ndial == 0:
                socket.bind(addr)
                _logger.info("Listening on %s.", addr)
            else:
                socket.connect(addr)
                _logger.info("Connected to %s - starting stream.", addr)
        except ZMQError as e:
            _logger.error("Unable to connect to %s - %s", addr, e)

        for msg in gen:
            socket.send(msg)
            yield len(msg)


def _drain_monitor(mon) -> list[dict]:
    """Non-blocking drain of all pending monitor events from a PAIR socket."""
    events = []
    while True:
        try:
            msg = mon.recv_multipart(flags=zmq.NOBLOCK)
            events.append(zmq_monitor.parse_monitor_message(msg))
        except zmq.Again:
            break
    return events # type: ignore[return-value]


@stream.source
def puller(addr: str, ndial: int) -> Iterator[bytes]:
    """Pull from addr using ndial parallel PULL sockets.

    ndial=0  bind to addr (single socket, listen mode)
    ndial=1  single connect (no overhead path)
    ndial=N  N parallel connections — ZMQ's C I/O threads receive on all
             simultaneously; a single Poller drains whichever sockets are
             ready, with no Python threads or queues in the way.

    With ndial > 1 the producer's PUSH socket round-robins messages across
    the N connections, spreading load across N independent TCP streams.
    Tune ndial to match what iperf3 -P N gives on the link.
    """
    assert ndial >= 0

    ctxt = zmq.Context.instance()
    n = max(ndial, 1)
    bind_mode = (ndial == 0)

    sockets: list = []
    monitors: list = []
    poller = zmq.Poller()

    try:
        for i in range(n):
            s = ctxt.socket(zmq.PULL)
            mon_addr = f"inproc://monitor.pull.{id(s)}"
            s.monitor(mon_addr, zmq.EVENT_DISCONNECTED | zmq.EVENT_CONNECTED)
            mon = ctxt.socket(zmq.PAIR)
            mon.connect(mon_addr)
            poller.register(s, zmq.POLLIN)
            poller.register(mon, zmq.POLLIN)
            sockets.append(s)
            monitors.append(mon)

        if bind_mode:
            try:
                sockets[0].bind(addr)
                _logger.info("Pull: waiting for connection")
            except ZMQError as e:
                _logger.error("Unable to bind to %s - %s", addr, e)
                return
        else:
            for i, s in enumerate(sockets):
                try:
                    s.connect(addr)
                    label = addr if n == 1 else f"{addr} [{i}]"
                    _logger.info("Connected to %s - starting recv.", label)
                except ZMQError as e:
                    _logger.error("Unable to connect to %s - %s", addr, e)

        # Per-socket connection tracking
        conn = [0] * n   # EVENT_CONNECTED count
        disc = [0] * n   # EVENT_DISCONNECTED count
        done: set[int] = set()

        sock_to_idx  = {id(s): i for i, s in enumerate(sockets)}
        mon_to_idx   = {id(m): i for i, m in enumerate(monitors)}

        while len(done) < n:
            ready = dict(poller.poll(1000))

            # Drain data from all ready data sockets
            for s in sockets:
                if s in ready:
                    while True:
                        try:
                            yield s.recv(flags=zmq.NOBLOCK)
                        except zmq.Again:
                            break

            # Process monitor events (drain each ready monitor fully)
            for mon in monitors:
                if mon not in ready:
                    continue
                i = mon_to_idx[id(mon)]
                for event in _drain_monitor(mon):
                    ev = event['event']
                    if ev == zmq.EVENT_CONNECTED:
                        conn[i] += 1
                        label = addr if n == 1 else f"{addr} [{i}]"
                        _logger.info("Source connected. (%s)", label)
                    elif ev == zmq.EVENT_DISCONNECTED:
                        disc[i] += 1
                        conn[i] = max(conn[i], disc[i])
                        label = addr if n == 1 else f"{addr} [{i}]"
                        _logger.info("Source disconnected. (%s)", label)
                        if conn[i] > 0 and conn[i] == disc[i]:
                            if bind_mode:
                                sockets[i].unbind(addr)
                            else:
                                sockets[i].disconnect(addr)
                            _logger.info("Shutting down... (%s)", label)

            # On 1-second timeout, check which sockets can be retired
            if len(ready) == 0:
                for i in range(n):
                    if i in done:
                        continue
                    if conn[i] == 0:
                        _logger.debug("Pull [%d]: waiting for connection", i)
                    elif conn[i] > disc[i]:
                        _logger.debug("Pull [%d]: slow input", i)
                    else:
                        # Disconnected — retire once receive buffer is empty
                        if sockets[i].getsockopt(zmq.EVENTS) == 0:
                            done.add(i)

    finally:
        for i, s in enumerate(sockets):
            try:
                s.disable_monitor()
            except Exception:
                pass
            monitors[i].close()
            s.close()
