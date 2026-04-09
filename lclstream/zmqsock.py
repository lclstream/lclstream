from typing import TypeVar, Optional, Union
from collections.abc import Iterator, Callable
import io
import time
import logging
_logger = logging.getLogger(__name__)

import stream
import h5py # type: ignore[import-untyped]
from zmq import ZMQError
import zmq
import zmq.utils.monitor as monitor

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
        # Set linger to 0 so socket closes immediately without waiting
        #socket.setsockopt(zmq.LINGER, 0)
        # Queue 5 messages
        #socket.setsockopt(zmq.SNDHWM, 5)

        # Don't queue messages until a receiver connects.
        socket.setsockopt(zmq.IMMEDIATE, 1)

        try:
            if ndial == 0:
                socket.bind(addr)
                _logger.info("Listening on %s.", addr)
            else:
                #for dial in range(ndial):
                #    socket.dial(addr, block=True)
                socket.connect(addr)
                _logger.info("Connected to %s - starting stream.", addr)
        except ZMQError as e:
            _logger.error("Unable to connect to %s - %s", addr, e)

        for msg in gen:
            socket.send(msg)
            yield len(msg)

def get_monitor_event(monitor_socket):
    """Helper to receive and parse a monitor event."""
    msg = monitor_socket.recv_multipart()
    event = monitor.parse_monitor_message(msg)
    return event

@stream.source
def puller(addr: str, ndial: int) -> Iterator[bytes]:
    assert ndial >= 0

    ctxt = zmq.Context.instance()

    with ctxt.socket(zmq.PULL) as socket:
        socket.monitor("inproc://monitor.pull", zmq.EVENT_DISCONNECTED | zmq.EVENT_CONNECTED)
        monitor_socket = ctxt.socket(zmq.PAIR)
        monitor_socket.connect("inproc://monitor.pull")

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        poller.register(monitor_socket, zmq.POLLIN)

        try:
            if ndial == 0:
                socket.bind(addr)
                _logger.info("Pull: waiting for connection")
            else:
                socket.connect(addr)
                _logger.info("Connected to %s - starting recv.", addr)
        except ZMQError as e:
            _logger.error("Unable to connect to %s - %s", addr, e)

        connections    = 0
        disconnections = 0
        active = True
        while active:
            socks = dict(poller.poll(1000))

            # Check for actual data
            if socket in socks:
                #yield socket.recv()
                while True:
                    try:
                        yield socket.recv(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
            
            # Check for socket events (Disconnects)
            if monitor_socket in socks:
                event = get_monitor_event(monitor_socket)
                if event['event'] == zmq.EVENT_CONNECTED:
                    _logger.info("\nSource connected.")
                    connections += 1
                if event['event'] == zmq.EVENT_DISCONNECTED:
                    _logger.info("\nSource disconnected. Shutting down...")
                    disconnections += 1
                    if connections > 0 and connections == disconnections \
                                and ndial == 0:
                        socket.unbind(addr)

            if len(socks) == 0:
                if connections == 0:
                    _logger.debug("Pull: waiting for connection")
                elif connections > disconnections:
                    _logger.debug("Pull: slow input")
                else:
                    events = socket.getsockopt(zmq.EVENTS)
                    #print(f"{events} events")
                    active = events > 0

        # Cleanup
        socket.disable_monitor()
        monitor_socket.close()
