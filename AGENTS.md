# Parquet.jl contributor guide

Read `HANDOFF.md` first when continuing the `rewrite/1.0` branch or preparing a release.
Read `docs/dev/architecture.md` before a change. Update `test/conformance/features.toml` only when the required evidence exists.

- Keep the public surface small and namespaced. Do not add exports.
- Preserve unknown Thrift fields, enum values, and page kinds.
- Check a resource limit before every metadata-directed allocation.
- Use explicit `return` statements in functions.
- Use `T[]` for empty typed arrays.
- Keep functions small. Keep one empty line between functions.
- Wrap every `Threads.@spawn` task with `errormonitor`.
- Use `@atomic` fields instead of `Atomic{T}`.
- Add a focused regression test for every fix.
- Confirm written files with at least one independent Parquet implementation.

Do not copy code from Parquet3.jl. That repository has no license. Parquet2.jl is MIT, but copied work needs attribution and a license notice.
