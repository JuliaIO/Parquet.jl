# Working on Parquet.jl

1. Read `docs/dev/architecture.md` and `test/conformance/features.toml`.
2. Identify the exact Parquet 2.13.0 clause and corpus fixtures for the change.
3. Add valid, invalid, and resource-limit tests.
4. Implement the smallest complete layer change.
5. Run the focused tests and the full Julia test suite.
6. Run the relevant external oracle before changing a feature status to complete.

Never infer format support from a successful package load or a local-only round trip.
