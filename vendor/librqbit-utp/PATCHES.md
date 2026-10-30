# Vendored librqbit-utp

Source: crates.io `librqbit-utp` 0.7.0, upstream commit
`c26f57b2debbe35ed0ace1ad419de529f7a5bf95`.
Repository: https://github.com/ikatson/librqbit-utp
License: Apache-2.0; the original LICENSE is retained.

This pinned copy supports rtrnt's shared DHT/uTP UDP sockets. Local changes:

- Public custom-transport constructor using the standard environment.
- LEDBAT congestion control with one-way-delay samples from received headers.
- Fixed sampled timestamp differences: record at receive time, echo unchanged.

Upstream CUBIC remains available. rtrnt explicitly selects LEDBAT.
