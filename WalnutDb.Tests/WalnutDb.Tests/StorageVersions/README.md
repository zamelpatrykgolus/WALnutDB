# Storage-version test layout

- `V1/` contains golden compatibility tests for the original WALv1/SSTv1 binary formats.
- `Migration/` contains metadata-only legacy-to-manifest migration and repair tests.
- `Safety/` contains crash-boundary, concurrency, maintenance, and locking tests that every storage version must pass.

Future binary formats must get a sibling directory (`V2/`, etc.) while all V1 readers and fixtures remain in the suite.

`WalnutDb.Tests.V2/` is an independent project covering the incremental format,
mixed v1/v2 migrations, fault injection and abrupt process termination. Shared
behavior tests run with both version parameters. Always run the entire solution,
not only the newest format project: `dotnet test WalnutDb.sln -c Release`.

The groups are also executed as independent projects: `WalnutDb.Tests.V1`,
`WalnutDb.Tests.Migration`, and `WalnutDb.Tests.Safety`. The main
`WalnutDb.Tests` project contains format-independent unit and stress tests.
