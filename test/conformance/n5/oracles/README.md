# N5 locked oracle image

The Dockerfile builds the test-only Linux/amd64 image that contains Parquet Java
1.17.1, Arrow Rust 59.2.0, Temurin 11.0.28+6, Maven 3.9.8, and Rust 1.96.1.
The image also binds Perl 5.38.2 and JSON::PP 4.16 for the evidence comparator.
Every downloaded archive has a checked digest. Build packages come from the
immutable Ubuntu `20260822T000000Z` snapshot at exact versions. The image contains
the complete Maven repository and Cargo vendor tree. Closed-world manifests also
cover the Cargo source replacement, installed Rust oracle, saved Rust channel,
dependency trees, exact toolchain record, validator, and harness source. Its
validator checks all content and runs both harnesses without network access.
The fixed `SOURCE_DATE_EPOCH=1787356800` controls image metadata. The Docker
exporter rewrites retained file times to that epoch. Volatile package-manager
logs and Maven resolver records are absent.

Build and validate a local image without publishing it:

```sh
test/conformance/n5/bootstrap-oracles.sh --output /tmp/oracles.lock
```

The local command exits with status 2 after successful validation. It does not
write a binding lock because a local image ID is not a registry `RepoDigest`.
Publishing is a separate external action. The bootstrap always makes a no-cache
Linux/amd64 build and checks it with `--network none` before any push. If the
output lock already exists, the clean image ID must equal its locked image ID.
After publication is authorized, pass the one permitted repository explicitly:

```sh
test/conformance/n5/bootstrap-oracles.sh \
  --output test/conformance/n5/oracles.lock \
  --publish ghcr.io/juliaio/parquet-jl-n5-oracles
```

Publication uses a staging tag derived from the validated local image ID. The
bootstrap pulls the resulting digest, verifies that it has the same image ID and
`RepoDigest`, and repeats the offline validation before it writes a lock.

The binding runner requires the exact 22-field lock schema, fixed public
repository, Linux/amd64 platform, toolchain pins, upstream revisions, dependency
trees, content manifests, corpus commit, and fixture hash. It may pull only that
exact public digest. It verifies the digest and content bindings before it starts
the full gate with `--network none`:

```sh
test/conformance/n5/run-oracles.sh \
  --lock test/conformance/n5/oracles.lock \
  --network none
```
