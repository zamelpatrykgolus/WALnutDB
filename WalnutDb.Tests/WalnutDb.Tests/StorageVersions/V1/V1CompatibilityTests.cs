using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests.StorageVersions.V1;

file sealed class V1Doc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

[Trait("StorageVersion", "V1")]
public sealed class V1CompatibilityTests
{
    // Frozen WALv1 bytes: BEGIN(tx=7), PUT(table=golden,key=one,JSON), COMMIT(1).
    // This fixture is deliberately not produced by the current WalWriter.
    private const string GoldenWalV1 = "EQAAAAEHAAAAAAAAAAkAAAAAAAAAoNn/rDwAAAACBwAAAAAAAAAGAAMAAAAgAAAAZ29sZGVub25leyJJZCI6Im9uZSIsIlZhbHVlIjoiZ29sZGVuLXYxIn1ohGvtDQAAAP8HAAAAAAAAAAEAAADORL7+";
    private const string GoldenSstV1 = "U1NUdjEAAAADAAAAJAAAAG9uZXsiSWQiOiJvbmUiLCJWYWx1ZSI6InNzdC1nb2xkZW4tdjEifQEAAAA=";

    [Fact]
    public async Task FrozenWalV1Fixture_IsReadableByCurrentRecovery()
    {
        var dir = NewDir();
        await File.WriteAllBytesAsync(Path.Combine(dir, "wal.log"), Convert.FromBase64String(GoldenWalV1));

        await using var db = Open(dir, new DatabaseOptions { CheckpointOnDispose = false });
        var table = await db.OpenTableAsync("golden", new TableOptions<V1Doc>());
        Assert.Equal("golden-v1", (await table.GetAsync("one"))?.Value);
    }

    [Fact]
    public async Task FrozenSstV1Fixture_IsReadableWithoutRewrite()
    {
        var dir = NewDir();
        var sstDir = Path.Combine(dir, "sst");
        Directory.CreateDirectory(sstDir);
        await File.WriteAllBytesAsync(Path.Combine(sstDir, "golden.sst"), Convert.FromBase64String(GoldenSstV1));

        await using var db = Open(dir, new DatabaseOptions { CheckpointOnDispose = false });
        var table = await db.OpenTableAsync("golden", new TableOptions<V1Doc>());
        Assert.Equal("sst-golden-v1", (await table.GetAsync("one"))?.Value);
    }

    [Fact]
    public async Task WalV1_WithoutManifest_RemainsReadable()
    {
        var dir = NewDir();
        var options = new DatabaseOptions { CheckpointOnDispose = false };

        await using (var db = Open(dir, options))
        {
            var table = await db.OpenTableAsync("legacy", new TableOptions<V1Doc>());
            await table.UpsertAsync(new V1Doc { Id = "one", Value = "from-wal-v1" });
            await db.FlushAsync();
        }

        DeleteManifest(dir);

        await using var reopened = Open(dir, options);
        var reopenedTable = await reopened.OpenTableAsync("legacy", new TableOptions<V1Doc>());
        var row = await reopenedTable.GetAsync("one");
        Assert.Equal("from-wal-v1", row?.Value);
        Assert.True(File.Exists(Path.Combine(dir, "CURRENT")));
    }

    [Fact]
    public async Task Manifest_Preserves_Base64Looking_Literal_TableName()
    {
        var dir = NewDir();
        await using (var db = Open(dir, new DatabaseOptions { CheckpointOnDispose = false }))
        {
            var table = await db.OpenTableAsync("YWJj", new TableOptions<V1Doc>());
            await table.UpsertAsync(new V1Doc { Id = "id", Value = "literal-name" });
            await db.CheckpointAsync();
        }

        await using var reopened = Open(dir, new DatabaseOptions { CheckpointOnDispose = false });
        var literal = await reopened.OpenTableAsync("YWJj", new TableOptions<V1Doc>());
        Assert.Equal("literal-name", (await literal.GetAsync("id"))?.Value);
    }

    [Fact]
    public async Task AmbiguousLegacySstName_CanBeResolvedWithoutRewritingData()
    {
        var dir = NewDir();
        var baseOptions = new DatabaseOptions { CheckpointOnDispose = false };
        await using (var db = Open(dir, baseOptions))
        {
            var table = await db.OpenTableAsync("Lw", new TableOptions<V1Doc>());
            await table.UpsertAsync(new V1Doc { Id = "id", Value = "literal-Lw" });
            await db.CheckpointAsync();
        }
        DeleteManifest(dir);

        var sst = Directory.GetFiles(Path.Combine(dir, "sst"), "*.sst").Single();
        var before = await File.ReadAllBytesAsync(sst);
        var mappedOptions = new DatabaseOptions
        {
            CheckpointOnDispose = false,
            LegacyTableNameMappings = new Dictionary<string, string> { ["Lw"] = "Lw" }
        };
        await using var reopened = Open(dir, mappedOptions);
        var literal = await reopened.OpenTableAsync("Lw", new TableOptions<V1Doc>());
        Assert.Equal("literal-Lw", (await literal.GetAsync("id"))?.Value);
        Assert.Equal(before, await File.ReadAllBytesAsync(sst));
    }

    private static WalnutDatabase Open(string dir, DatabaseOptions options) =>
        new(dir, options, new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));

    private static string NewDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "storage-v1", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void DeleteManifest(string dir)
    {
        var current = Path.Combine(dir, "CURRENT");
        if (File.Exists(current)) File.Delete(current);
        foreach (var file in Directory.EnumerateFiles(dir, "MANIFEST-*")) File.Delete(file);
    }
}
