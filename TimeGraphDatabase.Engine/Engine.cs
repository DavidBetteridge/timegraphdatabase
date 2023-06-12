namespace TimeGraphDatabase.Engine;

public class Engine<T> : IDisposable
{
    private readonly Storage _storage;
    private readonly UniqueIdLookup _uniqueLookup;
    private readonly NodeStorage _nodeStorage;
    private Func<T, string> _uniqueIdSelector;

    public Engine(Func<T, string> uniqueIdSelector)
    {
        _uniqueIdSelector = uniqueIdSelector;
        _storage = new Storage();
        _uniqueLookup = new UniqueIdLookup();
        _nodeStorage = new NodeStorage();
    }
    
    public async Task InsertNode(T newNode)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(newNode);
        var nodeId = await _nodeStorage.InsertAsync(json);
        var uniqueId = _uniqueIdSelector(newNode);
        await _uniqueLookup.InsertAsync(nodeId, uniqueId);
    }

    public async Task<T?> FindNodeFromUniqueId(string uniqueId)
    {
        var matches = _uniqueLookup.FindAsync(uniqueId);
        await foreach (var match in matches)
        {
            var json = await _nodeStorage.GetByIdAsync(match);
            var node = System.Text.Json.JsonSerializer.Deserialize<T>(json);
            if (_uniqueIdSelector(node) == uniqueId)
                return node;
        }

        return default(T);
    }

    public void Dispose()
    {
        _storage.Dispose();
        _uniqueLookup.Dispose();
        _nodeStorage.Dispose();
    }
}