# Supply Chain Audit Notes

These notes summarize the AI-assisted local source review performed while
setting up `cargo vet`.

Network access was unavailable in the sandbox, so importing Google and Mozilla
audit sets could not be completed. The cargo-vet store therefore uses generated
`safe-to-deploy` exemptions for the full dependency graph, with the direct
runtime dependencies reviewed locally from the Cargo registry cache.

## Direct Dependencies Reviewed

### bytes 1.12.0

Reviewed the local crate manifest and source for unsafe and ambient capability
usage. The crate is a byte buffer library. Unsafe code is concentrated in buffer
ownership, slicing, vtable, reference-counting, and `BufMut` initialization
paths. No filesystem, network, or process execution capabilities were found in
normal library code.

### serialport 4.9.0

Reviewed the local crate manifest and source for unsafe and ambient capability
usage. The crate intentionally wraps OS serial-port APIs on POSIX, Windows, and
Apple platforms. Unsafe code is concentrated in FFI calls, termios/ioctl
handling, handle ownership, and platform enumeration. Filesystem and registry
access is limited to serial-device discovery and OS metadata reads consistent
with the crate purpose. No process execution or network access was found.

### tokio 1.52.3

Reviewed the local crate manifest and enabled runtime, sync, and io-util source
areas relevant to this workspace. Unsafe code is concentrated in task
scheduling, synchronization primitives, waker handling, and low-level runtime
internals. This crate enables `rt`, `rt-multi-thread`, `sync`, and `io-util`;
it does not enable Tokio `net`, `fs`, `process`, `signal`, or `time` features.
