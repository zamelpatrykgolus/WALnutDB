namespace WalnutDb.Sst;

internal interface ITableReader : IDisposable
{
    string Path { get; }
    Task Retire();
    bool TryGet(ReadOnlySpan<byte> key, out byte[]? value);
    IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive);
}
