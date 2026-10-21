using System.Collections.Concurrent;

namespace NNostr.Client;

public class NostrClientPool : IDisposable
{
    private readonly ConcurrentDictionary<string, NostrClientWrapper> _clientPool = new();

    // Serializes acquisition (lookup + lease increment) against removal and
    // disposal (expiration cleanup and KillClient), so a wrapper can never be
    // disposed after a concurrent caller has acquired it.
    private readonly object _poolLock = new();

    private Timer? _cleanupTimer;
    private readonly TimeSpan _unusedClientTimeout;

    public NostrClientPool(TimeSpan? unusedClientTimeout = null)
    {
        _unusedClientTimeout = unusedClientTimeout ?? TimeSpan.FromMinutes(5);
        InitBackgroundTask();
    }

    protected virtual void InitBackgroundTask()
    {
        _cleanupTimer?.Dispose();
        _cleanupTimer = new Timer(CleanupExpiredClients, null, _unusedClientTimeout, _unusedClientTimeout);
    }

    public (INostrClient, IDisposable) GetClient(Uri[] relays)
    {
        if (relays.Length == 0)
            throw new ArgumentException("At least one relay is required", nameof(relays));

        var connString = GetConnString(relays);

        NostrClientWrapper clientWrapper;
        lock (_poolLock)
        {
            // Lookup and lease increment must be atomic with respect to
            // CleanupExpiredClients and KillClient: otherwise the timer could
            // observe an expired wrapper, then acquire the lock after our lookup
            // but before our increment, and remove and dispose the very wrapper
            // we are about to return.
            clientWrapper = _clientPool.GetOrAdd(connString,
                k => new NostrClientWrapper(relays.Length > 1
                    ? new CompositeNostrClient(relays)
                    : new NostrClient(relays[0])));

            clientWrapper.IncrementUsage();
        }

        return (clientWrapper.Client, new UsageDisposable(clientWrapper));
    }

    public async Task<(INostrClient, IDisposable)> GetClientAndConnect(Uri[] relays, CancellationToken token)
    {
        var result = GetClient(relays);
        try
        {
            await result.Item1.ConnectAndWaitUntilConnected(token, CancellationToken.None);
            return result;
        }
        catch (Exception e)
        {
            result.Item2.Dispose();
            KillClient(relays);
            throw;
        }
    }

    private string GetConnString(Uri[] relays)
    {
        return string.Join(';', relays.Select(r => r.ToString()));
    }

    public void KillClient(Uri[] relays)
    {
        var connstring = GetConnString(relays);
        lock (_poolLock)
        {
            if (_clientPool.TryRemove(connstring, out var clientWrapper))
            {
                clientWrapper.Dispose();
            }
        }
    }

    public void CleanupExpiredClients(object? state)
    {
        foreach (var key in _clientPool.Keys)
        {
            lock (_poolLock)
            {
                // Re-check under the lock: the entry may have been acquired or
                // removed since the key was enumerated.
                if (!_clientPool.TryGetValue(key, out var clientWrapper) || !clientWrapper.IsExpired())
                    continue;

                if (_clientPool.TryRemove(key, out clientWrapper))
                {
                    clientWrapper.Dispose();
                }
            }
        }
    }

    internal class UsageDisposable : IDisposable
    {
        internal readonly NostrClientWrapper ClientWrapper;

        public UsageDisposable(NostrClientWrapper clientWrapper)
        {
            ClientWrapper = clientWrapper;
        }

        public void Dispose()
        {
            ClientWrapper.DecrementUsage();
        }
    }

    public void Dispose()
    {
        _cleanupTimer?.Dispose();
        lock (_poolLock)
        {
            foreach (var client in _clientPool.Values)
            {
                client.Dispose();
            }
        }
    }
}