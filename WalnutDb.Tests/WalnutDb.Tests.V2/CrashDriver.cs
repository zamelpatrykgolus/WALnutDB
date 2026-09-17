using System.Diagnostics;
using WalnutDb.Core;

namespace WalnutDb.Tests.V2;

internal static class CrashDriver
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length is not (3 or 4)) return 2;
        bool armed = false;
        await using var db = Fixture.Open(args[0], new DatabaseOptions
        {
            CheckpointOnDispose = false, AllowStorageV2Upgrade = true,
            StorageFault = stage =>
            {
                if (armed && stage == args[2])
                {
                    Console.WriteLine("CRASH_READY:" + stage);
                    Console.Out.Flush();
                    Process.GetCurrentProcess().Kill();
                    Thread.Sleep(Timeout.Infinite); // no managed Dispose/finally recovery
                }
            }
        });
        if (args.Length == 3 || args[3] == "2") await db.UpgradeStorageAsync();
        var table = await Fixture.Table(db);
        await table.UpsertAsync(new() { Id = "deleted", Value = "before" });
        await db.CheckpointAsync();
        await using (var tx = await db.BeginTransactionAsync())
        {
            await table.DeleteAsync("deleted", tx);
            await table.UpsertAsync(new() { Id = "kept", Value = "durable" }, tx);
            await tx.CommitAsync();
        }
        if (args[1] == "compact") await db.CheckpointAsync();
        armed = true;
        if (args[1] == "compact") await db.CompactAsync("rows", new() { IncludeBaseSegments = true });
        else await db.CheckpointAsync();
        return 3; // requested crash point was not reached
    }
}

[Trait("StorageVersion", "V2")]
public sealed class ProcessCrashTests
{
    [Theory]
    [InlineData("checkpoint", "segment.after-record")]
    [InlineData("checkpoint", "segment.after-sync")]
    [InlineData("checkpoint", "segment.after-rename")]
    [InlineData("checkpoint", "manifest.after-sync")]
    [InlineData("checkpoint", "manifest.after-publish")]
    [InlineData("checkpoint", "wal.before-rotate")]
    [InlineData("checkpoint", "wal.after-rotate")]
    [InlineData("compact", "segment.after-record")]
    [InlineData("compact", "segment.after-rename")]
    [InlineData("compact", "cleanup.before-delete")]
    [InlineData("compact", "cleanup.after-delete")]
    [InlineData("compact", "cleanup.before-complete")]
    [InlineData("checkpoint", "segment.after-record", 1)]
    [InlineData("checkpoint", "segment.after-sync", 1)]
    [InlineData("checkpoint", "segment.after-rename", 1)]
    [InlineData("checkpoint", "manifest.after-sync", 1)]
    [InlineData("checkpoint", "manifest.after-publish", 1)]
    [InlineData("checkpoint", "wal.before-truncate", 1)]
    [InlineData("checkpoint", "wal.after-truncate", 1)]
    public async Task AbruptProcessTermination_DoesNotLoseDurableData(string operation, string point, int version = 2)
    {
        var dir = Fixture.Directory();
        var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(CrashDriver).Assembly.Location);
        start.ArgumentList.Add(dir); start.ArgumentList.Add(operation); start.ArgumentList.Add(point);
        start.ArgumentList.Add(version.ToString());
        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try { await process.WaitForExitAsync(timeout.Token); }
        catch { process.Kill(entireProcessTree: true); throw; }
        Assert.NotEqual(3, process.ExitCode);
        Assert.NotEqual(0, process.ExitCode);
        string output = await stdout; string errors = await stderr;
        Assert.True(output.Contains("CRASH_READY:" + point, StringComparison.Ordinal), output + errors);
        await using var reopened = Fixture.Open(dir);
        var table = await Fixture.Table(reopened);
        Assert.Null(await table.GetAsync("deleted"));
        Assert.Equal("durable", (await table.GetAsync("kept"))?.Value);
        if (operation == "compact")
            Assert.Contains(reopened.GetCompactionStatus()!.State, new[] { CompactionState.Completed, CompactionState.Failed });
        await reopened.CheckpointAsync();
    }
}
