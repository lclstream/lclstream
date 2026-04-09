# LCLStream

Image streaming client for
[LCLStreamer](https://slac-lcls.github.io/lclstreamer/).

This includes two basic features:

1. Request a data stream from LCLStreamer-API.

2. Receive a stream of data from LCLStreamer.

Python APIs are available for both, so that programs
can be written on top of them.

Alternately, [certified](https://github.com/ORNL/certified)'s
`message` command can be used to send the data request.
If successful, the LCLStreamer-API server will respond with
the nng URI at which the data is to be accessed.
With this method, the user is expected to arrange their own
nng reader from that URI.

    message -X POST https://sdfdtn003.slac.stanford.edu:4433/transfers lclstreamer.yaml


For convenience, the `lclstream` program sends the
data request and, if successful, emits the data in
tar format to stdout.  On a UNIX system, this can be
used as:

    lclstream get https://sdfdtn003.slac.stanford.edu:4433/transfers lclstreamer.yaml | tar xf -

It's also possible to skip the API request and receive (pull)
or send (push) a list of files directly to/from an nng URI:

    lclstream pull --dial tcp://127.0.0.1:3030 | tar xf -

or

    lclstream push --addr tcp://127.0.0.1:3030 *.h5


# Development

Install/test with uv

    uv sybc
    uv run --extra dev mypy lclstream
    uv run --extra dev pytest --cov lclstream tests
    uv run lclstream <...>


# Certified

Talking to the LCLStream-API requires mutual TLS authentication.
The necessary key management can be handled most easily through
[certified](https://certified.readthedocs.io/en/latest/).
