namespace WalnutDb;

public sealed record StorageMigrationPlan(int SourceVersion, int TargetVersion, long ExistingSstBytes,
    long DataBytesToRewrite, bool RequiresV2RollbackReader);

public enum CompactionState { Preparing, PublishedCleanupPending, Completed, Failed, Cancelled }

public sealed record CompactionStatus(string Id, string Table, CompactionState State, string? Detail = null);

public sealed class CompactionOptions
{
    /// <summary>False merges only deltas, keeping the large base and delete markers.</summary>
    public bool IncludeBaseSegments { get; init; }
    /// <summary>Maximum total input bytes. Refuse before changing the active set if exceeded.</summary>
    public long MaxInputBytes { get; init; } = 256L * 1024 * 1024;
    public long MaxOutputBytes { get; init; } = 256L * 1024 * 1024;
    /// <summary>Optional application write-rate limit; zero disables throttling.</summary>
    public long WriteBytesPerSecond { get; init; }
}

public sealed record StorageWriteStatistics(long Checkpoints, long SkippedCheckpoints,
    long SstBytesWritten, long CompactionBytesWritten);
