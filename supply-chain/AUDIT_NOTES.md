# Supply Chain Audit Notes

These notes summarize the AI-assisted local source review performed while
setting up `cargo vet`.

Shell network access was unavailable in the sandbox, so live Google and Mozilla
imports could not be refreshed reliably by plain `cargo vet`. The audit records
used from the cached Google import data were folded into `audits.toml`, and the
remaining crate versions are covered by local AI-assisted `safe-to-deploy`
audit records. No exemptions are used.

## Direct Dependencies Reviewed

### bytes 1.12.1

Reviewed the local crate manifest and source for unsafe and ambient capability
usage. The crate is a byte buffer library. Unsafe code is concentrated in buffer
ownership, slicing, vtable, reference-counting, and `BufMut` initialization
paths. No filesystem, network, or process execution capabilities were found in
normal library code. The 1.12.0 to 1.12.1 delta allocates shared state storage
before rebuilding vector-backed buffers so allocation panics do not drop rebuilt
vectors; no new ambient capabilities were introduced.

### serialport 4.9.0

Reviewed the local crate manifest and source for unsafe and ambient capability
usage. The crate intentionally wraps OS serial-port APIs on POSIX, Windows, and
Apple platforms. Unsafe code is concentrated in FFI calls, termios/ioctl
handling, handle ownership, and platform enumeration. Filesystem and registry
access is limited to serial-device discovery and OS metadata reads consistent
with the crate purpose. No process execution or network access was found.

### tokio 1.52.3

Reviewed the local crate manifest and enabled `sync`, `io-util`, and test-only
`rt` source areas relevant to this workspace. Unsafe code is concentrated in
task scheduling, synchronization primitives, waker handling, and low-level
runtime internals. The library dependency enables `sync` and `io-util`; tests
also enable `rt`. This workspace does not enable Tokio `rt-multi-thread`,
`net`, `fs`, `process`, `signal`, or `time` features.
