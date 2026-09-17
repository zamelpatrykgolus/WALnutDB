using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Diagnosers;

if (args.Length > 0 && args[0] == "--storage-audit")
{
    await WalnutDb.Bench.StorageWriteAudit.RunAsync(args.Length > 1 ? args[1] : "storage-write-audit.json");
    return;
}

var cfg = ManualConfig.Create(DefaultConfig.Instance)
    .AddJob(Job.Default.WithId(".NET 8"))
    .AddDiagnoser(MemoryDiagnoser.Default);

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, cfg);
