using System.Text;

namespace TimeGraphDatabase.Engine;

public class NodeStorage : IDisposable
{
    private readonly FileStream _indexFile;
    private readonly FileStream _contentFile;
    private long _contentFileSize;
    private long _indexFileSize;
    private readonly byte[] _contentPointerBuffer = new byte[8];
    private readonly byte[] _contentLengthBuffer = new byte[4];

    public NodeStorage()
    {
        var indexFilePath = "nodes.index";
        var contentFilePath = "nodes.content";
        
        _indexFile = File.Open(indexFilePath, FileMode.OpenOrCreate);
        _contentFile = File.Open(contentFilePath, FileMode.OpenOrCreate);
        
        _contentFileSize = _contentFile.Seek(0, SeekOrigin.End);
        _indexFileSize = _indexFile.Seek(0, SeekOrigin.End);
    }


    public async Task UpdateNodeAsync(long nodeId, string text)
    {
        var indexLocation = (nodeId - 1) * (8 + 4);
        if (_indexFile.Position != indexLocation)
            _indexFile.Seek(indexLocation, SeekOrigin.Begin);
        
        await _indexFile.ReadAsync(_contentPointerBuffer.AsMemory(0, 8));
        await _indexFile.ReadAsync(_contentLengthBuffer.AsMemory(0, 4));

        var contentPointer = BitConverter.ToInt64(_contentPointerBuffer);
        var contentLength = BitConverter.ToInt32(_contentLengthBuffer);

        var bytes = Encoding.Unicode.GetBytes(text);
        var updatedContentLength = bytes.Length;
        var updatedContentLengthBytes = BitConverter.GetBytes(updatedContentLength);
        
        if (updatedContentLength <= contentLength)
        {
            // We can do an in place update.
            if (_contentFile.Position != contentPointer)
                _contentFile.Seek(contentPointer, SeekOrigin.Begin);
            await _contentFile.WriteAsync(bytes);
            
            _indexFile.Seek(-4, SeekOrigin.Current);
            await _indexFile.WriteAsync(updatedContentLengthBytes);   // Int  (4 bytes)
        }
        else
        {
            // We don't have space to update it in place,  so we need to append it to the end 
            // of the contents file and updated the pointer.
            if (_contentFile.Position != _contentFileSize)
                _contentFile.Seek(0, SeekOrigin.End);
            await _contentFile.WriteAsync(bytes);
            
            var contentPointerBytes = BitConverter.GetBytes(_contentFileSize);
            _contentFileSize += updatedContentLength;
            
            _indexFile.Seek(-12, SeekOrigin.Current);
            await _indexFile.WriteAsync(contentPointerBytes);  // Long (8 bytes)
            await _indexFile.WriteAsync(updatedContentLengthBytes);   // Int  (4 bytes)
        }
    }
    

    public async Task<long> InsertAsync(string text)
    {
        var bytes = Encoding.Unicode.GetBytes(text);
        var contentLength = bytes.Length;

        // Write the content
        if (_contentFile.Position != _contentFileSize)
            _contentFile.Seek(0, SeekOrigin.End);
        await _contentFile.WriteAsync(bytes);

        // Write the index
        var contentPointerBytes = BitConverter.GetBytes(_contentFileSize);
        var contentLengthBytes = BitConverter.GetBytes(contentLength);
        if (_indexFile.Position != _indexFileSize)
            _indexFile.Seek(0, SeekOrigin.End);
        await _indexFile.WriteAsync(contentPointerBytes);  // Long (8 bytes)
        await _indexFile.WriteAsync(contentLengthBytes);   // Int  (4 bytes)

        _contentFileSize += contentLength;
        _indexFileSize += 12;
        
        var indexFileSize = _indexFile.Position;
        return indexFileSize / (8 + 4);  // 1 based node Id
    }

    public async Task<string> GetByIdAsync(long nodeId)
    {
        var indexLocation = (nodeId - 1) * (8 + 4);
        if (_indexFile.Position != indexLocation)
            _indexFile.Seek(indexLocation, SeekOrigin.Begin);
        
        await _indexFile.ReadAsync(_contentPointerBuffer.AsMemory(0, 8));
        await _indexFile.ReadAsync(_contentLengthBuffer.AsMemory(0, 4));

        var contentPointer = BitConverter.ToInt64(_contentPointerBuffer);
        var contentLength = BitConverter.ToInt32(_contentLengthBuffer);

        var contentBuffer = new byte[contentLength];  // We could try pro-allocating this buffer
        if (_contentFile.Position != contentPointer)
            _contentFile.Seek(contentPointer, SeekOrigin.Begin);
        await _contentFile.ReadAsync(contentBuffer.AsMemory(0, contentLength));

        return Encoding.Unicode.GetString(contentBuffer);
    }

    public void Dispose()
    {
        _indexFile.Dispose();
        _contentFile.Dispose();
    }
}