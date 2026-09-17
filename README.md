# WALnutDB

> A simple, safe embedded database for .NET 8+. Built for devices with little RAM and slow flash (SD/eMMC). It uses Write-Ahead Logging (WAL) for power-loss durability, compacts data into sorted segment files (SST), and provides secondary indexes and time-series scans. Fully managed (no native deps), cross-platform, and async by default.

![WALnutDB mascot](https://raw.githubusercontent.com/patryk9200/WALnutDB/refs/heads/master/logo-small.jpg)

[![NuGet](https://img.shields.io/nuget/v/WALnutDB.svg)](https://www.nuget.org/packages/WALnutDB)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)
[![.NET](https://img.shields.io/badge/.NET-8.0+-512BD4.svg)](https://dotnet.microsoft.com/)

## Storage v2 (package 2.0.0)

Storage v2 is opt-in. Existing SSTv1 files remain readable and become immutable
base segments; new checkpoints write small SSTv2 deltas with checksums and explicit
delete markers. V1 remains the default. Enable `AllowStorageV2Upgrade` and call
`WalnutDatabase.UpgradeStorageAsync()` only after all application versions allowed
for rollback support v2. `PlanStorageUpgradeAsync()` reports the migration plan
without rewriting data. Older 1.0.x software must not open an upgraded database.

`CompactAsync()` supports input/output/rate budgets and reports `Completed` only
after durable publication and required cleanup. Interrupted cleanup resumes on
open. See [migration, recovery and compaction](WalnutDb/Documentation/PL/storage-v2.md).
Tests run both formats on Windows and Linux; physical power-cut validation on the
target eMMC/filesystem remains a deployment requirement.

## Highlights

- **Power-loss safety:** WAL with per-frame CRC, truncation-tolerant replay.
- **Async I/O everywhere:** designed for slow media and small RAM.
- **Document tables:** pluggable serialization (e.g., `System.Text.Json`); GUID/string/byte[] keys.
- **Secondary indexes:** range scans over string/bytes/ints/floats/decimal (with scale).
- **Time-series mode:** UTC-ordered keys, efficient range scans by time.
- **Checkpoint & SST:** flush memtables to sorted segments, fast reads, WAL truncation.
- **Cross-platform:** Linux/Windows/macOS; file-per-table for fault isolation.
- **No native dependencies:** pure C# for easy deployment on IoT.

> **Roadmap:** online compaction/defrag stats & swap, unique index enforcement, optional encryption-at-rest, richer query push-downs.

---

## Install

```bash
dotnet add package WALnutDB
```

Target framework: **.NET 8.0+**

---

## Quick Start

```csharp
using System.Text.Json;
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

// Sample POCO
public sealed class User
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Age")]   public int Age { get; set; }
    public string Name { get; set; } = "";
}

// Create database directory
var dir = Path.Combine(Path.GetTempPath(), "walnut-demo");
Directory.CreateDirectory(dir);

// WAL writer (group-commit, fsync per batch)
await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));

// DB (managed, no native deps)
await using var db = new WalnutDatabase(
    directory: dir,
    options: new DatabaseOptions(),
    manifest: new FileSystemManifestStore(dir),
    wal: wal
);

// Open a table (default serializer is used unless you override it)
var users = await db.OpenTableAsync(new TableOptions<User>
{
    GetId = u => u.Id, // string / Guid / byte[] supported
    // Optional custom serializer:
    //Serialize   = u => JsonSerializer.SerializeToUtf8Bytes(u),
    //Deserialize = b => JsonSerializer.Deserialize<User>(b.Span)!,
    StoreGuidStringsAsBinary = true // optional optimization
});

// Upsert + Get
await users.UpsertAsync(new User { Id = "u1", Name = "Ada", Age = 37 });
var ada = await users.GetAsync("u1");
Console.WriteLine($"{ada?.Id}: {ada?.Name} ({ada?.Age})");

// Enumerate all
await foreach (var u in users.GetAllAsync())
    Console.WriteLine($"{u.Id}: {u.Name} ({u.Age})");

// Durability checkpoint (flush mem→SST, truncate WAL)
await db.CheckpointAsync();
```

---

## Secondary Indexes

Declare indexes:

```csharp
public sealed class Product
{
    [DatabaseObjectId] public string Id { get; set; } = "";

    [DbIndex("Category")]
    public string Category { get; set; } = "";

    [DbIndex("Price", decimalScale: 2)] // decimals require a fixed scale
    public decimal Price { get; set; }
}
```

Open table and scan ranges (decimal + string prefix):

```csharp
using System.Text.Json;
using WalnutDb.Indexing;

var products = await db.OpenTableAsync(new TableOptions<Product> {
    GetId = p => p.Id,
    // Optional custom serializer:
    //Serialize   = p => JsonSerializer.SerializeToUtf8Bytes(p),
    //Deserialize = b => JsonSerializer.Deserialize<Product>(b.Span)!
});

// Insert a few
await products.UpsertAsync(new Product { Id = "A", Category = "sensors",   Price = 12.50m });
await products.UpsertAsync(new Product { Id = "B", Category = "sensors",   Price = 18.75m });
await products.UpsertAsync(new Product { Id = "C", Category = "actuators", Price = 33.40m });

// Range by decimal index (inclusive/exclusive semantics)
var from = IndexKeyCodec.Encode(10m, decimalScale: 2);
var to   = IndexKeyCodec.Encode(20m, decimalScale: 2);

await foreach (var p in products.ScanByIndexAsync("Price", from, to))
    Console.WriteLine($"{p.Id} {p.Price:0.00}");

// Range by string prefix (e.g., category "sensors")
var start = IndexKeyCodec.Encode("sensors");
var end   = IndexKeyCodec.PrefixUpperBound(start);

await foreach (var p in products.ScanByIndexAsync("Category", start, end))
    Console.WriteLine($"{p.Id} {p.Category}");
```

---

## Index hints

You can steer scans using `IndexHint` factories. (Descending `Asc=false` is planned; current implementation returns ascending.)

```csharp
using WalnutDb.Indexing;

// Price in [10.00, 20.00), ascending, skip first 5, take 10
var hint = IndexHint.FromValues("Price", 10.00m, 20.00m, decimalScale: 2, asc: true, skip: 5, take: 10);
await foreach (var p in products.QueryAsync(x => true, hint))
    Console.WriteLine($"{p.Id} {p.Price:0.00}");

// Prefix by category:
var hint2 = IndexHint.FromPrefix("Category", "sensors");
await foreach (var p in products.QueryAsync(_ => true, hint2))
    Console.WriteLine($"{p.Id} {p.Category}");
```

---

## Unique indexes

Mark a property with `[DbIndex(Name, Unique = true)]`. `null` values do **not** participate in uniqueness (SQL-like behavior).

```csharp
public sealed class User
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)]
    public string? Email { get; set; }
}

// Insert A with email X, then checkpoint
await users.UpsertAsync(new User { Id = "A", Email = "x@example.com" });
await db.CheckpointAsync();

// Inserting B with the same email throws
await Assert.ThrowsAsync<InvalidOperationException>(() =>
    users.UpsertAsync(new User { Id = "B", Email = "x@example.com" }));

// Deleting A releases the unique constraint immediately (in mem)
// and after checkpoint (for SST-backed entries)
await users.DeleteAsync("A");
await db.CheckpointAsync();
await users.UpsertAsync(new User { Id = "B", Email = "x@example.com" });
```

---

## Time-Series

Provide a **series id** and a **UTC timestamp**. Keys are encoded so that lexicographic byte order matches chronological order.

```csharp
public sealed class SensorSample
{
    [DatabaseObjectId] public string Id { get; set; } = ""; // optional
    public string  DeviceId { get; set; } = "";
    public DateTime Utc     { get; set; }       // must be UTC
    public double  Temperature { get; set; }
}

var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<SensorSample>
{
    GetSeriesId     = s => s.DeviceId,
    GetUtcTimestamp = s => s.Utc,
    // Optional custom serializer:
    //Serialize       = s => JsonSerializer.SerializeToUtf8Bytes(s),
    //Deserialize     = b => JsonSerializer.Deserialize<SensorSample>(b.Span)!,
});

// Append
await ts.AppendAsync(new SensorSample { DeviceId = "dev-1", Utc = DateTime.UtcNow, Temperature = 22.3 });

// Query by time range
var fromUtc = DateTime.UtcNow.AddMinutes(-10);
var toUtc   = DateTime.UtcNow;

await foreach (var s in ts.QueryAsync("dev-1", fromUtc, toUtc))
    Console.WriteLine($"{s.Utc:o} {s.Temperature:0.0}");
```

---

## Transactions

Both automatic **(implicit)** and manual **(explicit)** transactions are supported.

```csharp
// Auto: each call is an individual transaction
await users.UpsertAsync(new User { Id = "u2", Name = "Linus", Age = 49 });

// Manual: batch multiple ops → single WAL fsync
await using (var tx = await db.BeginTransactionAsync())
{
    await users.UpsertAsync(new User { Id = "u3", Name = "Grace", Age = 44 }, tx);
    await users.DeleteAsync("u1", tx);
    await tx.CommitAsync(Durability.Group);
}
```

---

## Encryption at rest

WALnutDB can encrypt values at rest (WAL + SST) with **AES-GCM(256)**. Keys and index keys remain plaintext for sortability.

**Encrypted:** table values written to WAL and SST.  
**Not encrypted:** primary keys, index keys (they include the index value), filenames/metadata.  
**Threat model:** protects against offline reads of WAL/SST files. In-memory data is plaintext.

```csharp
var dir = Path.Combine(Path.GetTempPath(), "walnut-enc");
await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));

var key = Convert.FromHexString("00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF");

await using var db = new WalnutDatabase(
    dir,
    new DatabaseOptions { Encryption = new AesGcmEncryption(key) },
    new FileSystemManifestStore(dir),
    wal);

var tbl = await db.OpenTableAsync(new TableOptions<MyDoc> { GetId = d => d.Id });
await tbl.UpsertAsync(new MyDoc { Id = "x", Secret = "hello" });
await db.CheckpointAsync(); // values written to SST are ciphertext
```

**Crash-recovery with encryption:** recovery verifies `crc32` and replays committed frames, decrypting values on the fly before applying them to memtables.

---

## Durability & Checkpoints

- `await db.FlushAsync()` — fsync WAL (fast).  
- `await db.CheckpointAsync()` — flush memtables → SST and **truncate the WAL**.  
- Recovery reads `wal.log`, verifies the CRC of each frame, replays **committed** transactions, and safely stops at a torn tail.

For safe shutdown on embedded devices:

```csharp
await db.CheckpointAsync();  // shortens recovery time and shrinks the WAL
```

---

## Preflight Checks

Verify permissions, free space, and ability to acquire exclusive locks:

```csharp
var report = await db.PreflightAsync(dir, reserveBytes: 4 * 1024 * 1024);
Console.WriteLine($"{report.FileSystem} free={report.FreeBytes} CanExclusive={report.CanExclusiveLock}");
```

---

## Benchmarks

In this repo there is **BenchmarkDotNet** (`WalnutDb.Bench`). Run in **Release**:

```bash
dotnet run -c Release --project WalnutDb.Bench
```

---

## On-disk formats (v1)

### WAL (v1)

Stream of frames:

```
[len:U32_LE] [payload:len bytes] [crc32:U32_LE]
```

- `crc32` is computed over `payload` (polynomial `0xEDB88320`).
- `payload` layout varies by op:
  - `Begin`: `[op:1][txId:U64][seqNo:U64]`
  - `Put`:   `[op:1][txId:U64][tlen:U16][klen:U32][vlen:U32][table:tlen][key:klen][value:vlen]`
  - `Del`:   `[op:1][txId:U64][tlen:U16][klen:U32][table:tlen][key:klen]`
  - `Commit`:`[op:1][txId:U64][opsCount:U32]` (reserved)
- Replay stops on the first malformed or truncated frame (“crash tail”).
- If **encryption** is enabled, **values** inside WAL frames are encrypted (AES‑GCM v1) with AAD=`[table|pk]`.

### SST (v1)

Flat sorted segment file:

```
Header:  "SSTv1\0\0\0"  (8 bytes)
Records: repeated [klen:U32_LE][vlen:U32_LE][key:klen][value:vlen]
Trailer: [count:U32_LE] (optional in future versions)
```

- Keys are sorted lexicographically; merges assume sorted input.
- If **encryption** is enabled, **values** stored in SST are ciphertext (AES‑GCM v1, same AAD).

### Filenames

Each logical table name is encoded to a filesystem‑safe base64‑url (without padding) when stored on disk. Example: `orders` → `b3JkZXJz`.

---

## Notes & Limitations

- `IndexHint.Asc=false` (descending scans) is planned; current implementation returns ascending.
- Unique indexes: `null` value does not participate in uniqueness.
- One SST per table in v1 (simple model suitable for embedded; background compaction will come later).

---

## License

MIT © WALnutDB contributors
