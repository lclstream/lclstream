import os
from io import BytesIO
import signal
import random
from pathlib import Path
import time

import h5py  # type: ignore

import pytest
from typer.testing import CliRunner
from lclstream.lclstream import app
from lclstream.zmqsock import puller, pusher

from contextlib import contextmanager

@contextmanager
def child_process(fn, *args, **kws):
    """ Create a child process and run fn.

        After context completes, the child
        process is sent a SIGTERM.
    """
    child_pid = os.fork()
    if child_pid: # parent process yields
       try:
           yield
       finally:
           time.sleep(1)
           os.kill(child_pid, signal.SIGTERM)
    else: # child process
       fn( *args, **kws)
       os._exit(0)

def gen_data(dirname, n=100, m=1000):
    """ Generate n files with m bytes each.
    Return a list of files
    """
    fnames = []
    dirname.mkdir()#exist_ok=True)
    for i in range(n):
        # not h5 format, but push/pull don't mind
        fname = dirname/f"{i:02d}.h5"
        with open(fname, "wb") as f:
            f.write( random.randbytes(m) )
        fnames.append(str(fname))
    return fnames

def check_data(fnames, dirname):
    for n in fnames:
        fname = Path(n)
        out = dirname / fname.name
        x = open(fname, "rb").read()
        y = open(out, "rb").read()
        assert x == y, f"{fname} and {out} differ."

def run_pull(output, **args):
    output.mkdir()#exist_ok=True)
    for i,x in enumerate(puller(**args)):
        # not h5 format, but push/pull don't mind
        fname = output/f"{i:02d}.h5"
        with open(fname, "wb") as f:
            f.write( x )
        print(f"wrote {len(x)} bytes to {fname}")

def run_push(fnames, **args):
    def genz(fnames):
        for fname in fnames:
            with open(fname, "rb") as f:
                yield f.read()
    num = 0
    for n in genz(fnames) >> pusher(**args):
        num += 1
    assert num == len(fnames)

def test_push(tmpdir):
    inp = tmpdir / "input"
    out = tmpdir / "output"
    fnames = gen_data(inp, 100, 1024)

    addr = "tcp://127.0.0.1:50201"
    with child_process(run_pull, out, addr=addr, ndial=0):
        run_push(fnames, addr=addr, ndial=1)
    check_data(fnames, out)

def test_pull(tmpdir):
    inp = tmpdir / "input"
    out = tmpdir / "output"
    fnames = gen_data(inp, 100, 1024)

    addr = "tcp://127.0.0.1:50301"
    with child_process(run_push, fnames, addr=addr, ndial=0):
        run_pull(out, addr=addr, ndial=1)
    check_data(fnames, out)

runner = CliRunner()

def xtest_push(tmpdir):
    inp = tmpdir / "input"
    out = tmpdir / "output"
    fnames = gen_data(inp, 100, 1024)
    # Setup test data for send/recv.

    addr = "tcp://127.0.0.1:50202"
    with child_process(run_pull, out, addr=addr, ndial=0):
        result = runner.invoke(app, ["push", "-n", "1",
                                     "--addr", addr,
                                    ] + fnames)
    assert result.exit_code == 0
    print(result.stdout)
    print("---")
    print(result.output)
    check_data(fnames, out)

def xtest_pull(tmpdir):
    inp = tmpdir / "input"
    out = tmpdir / "output"
    fnames = gen_data(inp, 100, 1024)
    addr = "tcp://127.0.0.1:50203"
    with child_process(run_push, fnames, addr=addr, ndial=0):
        result = runner.invoke(app, ["pull", "--ndial", "1",
                                     "--dial", addr,
                                     "--names", "%02d.h5",
                                    ])

    assert result.exit_code == 0
    print(result.stdout)
    print("---")
    print(result.output)
    check_data(fnames, out)

def xtest_badpull():
    """ Incorrect ways to run pull """

    addr = "tcp://127.0.0.1:50204"
    result = runner.invoke(app, ["pull", "--ndial", "0",
                                 "--dial", addr,
                                 "--names", "%02d.h5",
                                ])
    assert result.exit_code != 0

    result = runner.invoke(app, ["pull", "--ndial", "0",
                                 "--dial", addr ])
    assert result.exit_code != 0

    result = runner.invoke(app, ["pull", "--listen", addr,
                                 "--dial", addr ])
    assert result.exit_code != 0

def xtest_badpush():
    """ Incorrect ways to run pull """

    addr = "tcp://127.0.0.1:50204"
    result = runner.invoke(app, ["push", "--ndial", "0",
                                 "--addr", addr ])
    assert result.exit_code != 0
