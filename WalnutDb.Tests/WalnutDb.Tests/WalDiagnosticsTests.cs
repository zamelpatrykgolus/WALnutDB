using System;
using System.IO;
using System.Threading.Tasks;
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Diagnostics;
using WalnutDb.Wal;
using Xunit;

namespace WalnutDb.Tests;

public sealed class WalDiagnosticsTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "wal-diag", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class DiagDoc
    {
        public string Id { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Fact]
    public async Task Scan_Detects_Repeated_Puts_For_Same_Key()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions { CheckpointOnDispose = false }, new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync<DiagDoc>("diag_docs", new TableOptions<DiagDoc> { GetId = d => d.Id });

            for (int i = 0; i < 3; i++)
                await table.UpsertAsync(new DiagDoc { Id = "dup", Value = i });

            await db.FlushAsync();
        }

        var report = WalDiagnostics.Scan(walPath, tailHistory: 0);

        Assert.Contains(report.RepeatedKeys, rk =>
            rk.Table == "diag_docs"
            && rk.PutCount >= 3
            && rk.IsPutOnly
            && rk.NetPutCount >= 3
            && rk.KeyHex.StartsWith("647570", StringComparison.OrdinalIgnoreCase));
    }
}
