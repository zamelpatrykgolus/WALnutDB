using WalnutDb.Core;
using WalnutDb.Storage;

namespace WalnutDb.Sst;

internal sealed class SegmentSetReader : ITableReader
{
    private readonly SegmentReader[] _readers;
    private readonly object _sync = new();
    private int _users;
    private bool _retired;
    private TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public string Path { get; }
    public IReadOnlyList<SegmentFile> Files { get; }

    public SegmentSetReader(string directory, IReadOnlyList<SegmentFile> files, IDictionary<SegmentFile, SegmentReader>? cache = null)
    {
        Files = files.ToArray();
        Path = System.IO.Path.Combine(directory, files[0].FileName);
        _readers = files.Select(f =>
        {
            if (cache is not null && cache.TryGetValue(f, out var reader)) return reader;
            var created = new SegmentReader(directory, f);
            if (cache is not null) cache[f] = created;
            return created;
        }).ToArray();
    }

    private IDisposable Pin()
    {
        lock (_sync)
        {
            if (_retired) throw new ObjectDisposedException(nameof(SegmentSetReader));
            _users++;
            return new Lease(this);
        }
    }
    private sealed class Lease(SegmentSetReader owner) : IDisposable
    {
        public void Dispose() { lock (owner._sync) { if (--owner._users == 0 && owner._retired) owner._drained.TrySetResult(); } }
    }
    public Task Retire()
    {
        lock (_sync) { _retired = true; if (_users == 0) _drained.TrySetResult(); return _drained.Task; }
    }
    public void Dispose() => Retire();

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        using var lease = Pin();
        var target = key.ToArray();
        for (int i = _readers.Length - 1; i >= 0; i--)
        {
            using var iterator = _readers[i].Scan(target).GetEnumerator();
            if (!iterator.MoveNext() || !iterator.Current.Key.AsSpan().SequenceEqual(target)) continue;
            value = iterator.Current.Deleted ? null : iterator.Current.Value;
            return !iterator.Current.Deleted;
        }
        value = null;
        return false;
    }

    public IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive)
    {
        foreach (var row in ScanRows(fromInclusive, toExclusive))
            if (!row.Deleted) yield return (row.Key, row.Value);
    }

    public IEnumerable<SegmentRow> ScanRows(byte[]? from = null, byte[]? to = null)
    {
        using var lease = Pin();
        var iterators = _readers.Select(r => r.Scan(from, to).GetEnumerator()).ToArray();
        var active = new bool[iterators.Length];
        try
        {
            for (int i = 0; i < iterators.Length; i++) active[i] = iterators[i].MoveNext();
            while (true)
            {
                int best = -1;
                for (int i = 0; i < iterators.Length; i++)
                    if (active[i] && (best < 0 || ByteArrayComparer.Instance.Compare(iterators[i].Current.Key, iterators[best].Current.Key) <= 0)) best = i;
                if (best < 0) yield break;
                var row = iterators[best].Current; // equal keys: newest (highest index) wins
                yield return row;
                for (int i = 0; i < iterators.Length; i++)
                    if (active[i] && ByteArrayComparer.Instance.Equals(iterators[i].Current.Key, row.Key)) active[i] = iterators[i].MoveNext();
            }
        }
        finally { foreach (var iterator in iterators) iterator.Dispose(); }
    }
}
