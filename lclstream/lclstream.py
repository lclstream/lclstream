#!/usr/bin/env python3

import time
from typing import Annotated, Optional, List, Tuple, Dict, Any
from collections.abc import Iterator
from pathlib import Path
import asyncio
import json
import sys
import signal

import stream
import typer
import yaml
from certified import Certified

from .nng import puller, pusher
from .stream_utils import clock
from .stream_tar import write_tar

from lclstream import __version__

app = typer.Typer()

def readfile(fname: Path) -> bytes:
    return fname.read_bytes()

@app.command()
def pull(listen: Annotated[
            Optional[str],
            typer.Option("--listen", "-l", help="Address to listen at (URL format)."),
        ] = None,
        dial: Annotated[
            Optional[str],
            typer.Option("--dial", "-d", help="Address to dial (URL format)."),
        ] = None,
        ndial: Annotated[
            Optional[int],
            typer.Option("--ndial", "-n", help="Number of simultaneous connections (dial only)"),
        ] = None,
        names: Annotated[
            str,
            typer.Option(help="Naming scheme for output files (must contain a single numeric format specifier like %d or %x)"),
        ] = "%05d.h5",
        quiet: Annotated[
            bool,
            typer.Option("--quiet", "-q", help="Quiet. Don't output to stderr."),
        ] = False,
    ) -> None:
    """
    Pull data from an open nng stream, printing as
    a tarfile format to stdout.
    """

    assert (dial is None) != (listen is None), "Use either dial or listen to specify an address."

    if listen is None:
        addr = dial
    else:
        assert ndial is None or ndial == 0, "Invalid ndial for listening mode"
        ndial = 0
        addr = listen

    if ndial is None: # wasn't set above, must be dialing
        ndial = 1

    inp = puller(addr, ndial)
    if not quiet: # add a display?
        inp >>= display_sz

    inp >> write_tar(sys.stdout, names)

# TODO tee to a Sink:
#@stream
#def tee(inp: Iterator[A], f: Sink[A]) -> Iterator[A]:

@app.command()
def push(names: Annotated[
            List[Path],
            typer.Argument(help="File paths."),
        ],
        addr: Annotated[str,
            typer.Option(help="Address to dial/listen at (URL format)."),
        ] = "tcp://127.0.0.1:3030",
        ndial: Annotated[
            int,
            typer.Option("--ndial", "-n", help="Dial-out to address if >0."),
        ] = 0,
    ) -> None:
    """ Push a list of files to an nng stream.
    Used to replay a data transmission.
    """
    
    messages = names >> stream.map(readfile) \
                     >> display_sz \
                     >> pusher(addr, ndial) \
                     >> clock()
    # run the stream
    final = messages >> stream.last()
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    if final['wait'] == 0: # prevent divide by zero exception [sic]
        final['wait'] = -1
    print(f"Sent {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.",
          file=sys.stderr
    )


@stream.stream
def display_sz(inp: Iterator[bytes]) -> Iterator[bytes]:
    # Note - it's also possible to write this as a composition
    # of stream operators...
    #yield from (inp >> a >> b)
    items = {'count': 0,
             'wait': 0.0,
             'size': 0
            }
    t0 = time.time() # to time the wait for input data
    for i, x in enumerate(inp):
        t1 = time.time()
        sz = len(x)
        items['count'] += 1
        items['wait'] += t1-t0
        items['size'] += sz
        if i % 10 == 9:
            print(f"At {items['count']}, {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.", file=sys.stderr)
        yield x
        t0 = time.time()

"""
    try:
        final = stats >> stream.last(-1)
    except IndexError:
        final = items
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    if final:
        print(f"Received {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.", file=sys.stderr)
    else:
        print("Stream completed with no data.", file=sys.stderr)
"""

@app.command()
def get(config: Annotated[
            Path,
            typer.Argument(help="LCLStreamer configuration file (json or yaml format)"),
        ],
        server: Annotated[
            str,
            typer.Option(help="API server name (Certified URL format)."),
        ] = "https://sdfdtn003.slac.stanford.edu:4433",
        ndial: Annotated[
            int,
            typer.Option(help="Number of parallel PULL connections."),
        ] = 16
    ) -> None:
    """
    Request a data stream from LCLStreamer-API
    and write the contents as a tarfile to stdout.
    """

    if config.suffix == ".yml" or config.suffix == ".yaml":
        cfg = yaml.safe_load( config.read_text() )
    else:
        cfg = json.loads( config.read_text() )
    #print(json.dumps(cfg, indent=2))

    # ask lclstream-api politely for data
    cert = Certified()
    headers = { "user-agent": f"lclstream/{__version__}",
                "Accept": "application/json" }

    async def request_data() -> Tuple[bool, Dict[str,Any]]:
        async with cert.ClientSession(
                        base_url = server,
                        headers = headers,
                    ) as cli:
            resp = await cli.post("/v1/transfers", json=cfg)
            ans = await resp.json()
        return resp.status, ans

    async def cancel_transfer(tid: str) -> None:
        async with cert.ClientSession(
                        base_url = server,
                        headers = headers,
                    ) as cli:
            await cli.delete(f"/v1/transfers/{tid}")

    status, ans = asyncio.run(request_data())

    # parse response from server
    url: str = ans.get("url", "")
    tid: str = ans.get("id", "")
    if status//100 != 2:
        print("Unable to download stream.\n"
              f"{status}: {ans}", file=sys.stderr)
        sys.exit(1)
    print("Received " + json.dumps(ans, indent=2), file=sys.stderr)

    def kill_transfer(sig, frame):
        nonlocal tid
        if tid != "":
            asyncio.run(cancel_transfer(tid))
        sys.exit(1)

    if url == "":
        print("No URL in response.", file=sys.stderr)
        kill_transfer()

    signal.signal(signal.SIGINT, kill_transfer)
    signal.signal(signal.SIGPIPE, kill_transfer)
    signal.signal(signal.SIGTERM, kill_transfer)

    # stream the response to stdout
    try:
        pull(dial=url, ndial=16)
    except Exception:
        kill_transfer(None, None)
        raise
