namespace NNostr.Client;

public class NostrClientWrapper : IDisposable
{
    public INostrClient Client { get; private set; }
    internal int UsageCount = 0;
    private int _isDisposed;
    private DateTimeOffset _lastUsed;

    public NostrClientWrapper(INostrClient client)
    {
        Client = client;
        _lastUsed = DateTimeOffset.UtcNow;
    }

    public void IncrementUsage()
    {
        _lastUsed = DateTimeOffset.UtcNow;
        Interlocked.Increment(ref UsageCount);
    }

    public void DecrementUsage()
    {
        _lastUsed = DateTimeOffset.UtcNow;
        if (Interlocked.Decrement(ref UsageCount) == 0 && IsExpired())
        {
            Dispose();
        }
    }

    public bool IsExpired()
    {
        // Only expire when nothing holds a lease: a long-lived consumer (eg. an NWC
        // invoice listener) keeps UsageCount above zero without calling
        // Increment/DecrementUsage again, and must not have its client disposed
        // underneath it by the cleanup timer.
        return UsageCount <= 0 && DateTimeOffset.UtcNow - _lastUsed > TimeSpan.FromMinutes(5);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            Client.Dispose();
        }
    }
}