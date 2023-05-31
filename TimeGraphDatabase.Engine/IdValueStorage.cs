using System.Text;

namespace TimeGraphDatabase.Engine;

public class IdValueStorage : IDisposable
{
    private readonly FileStream _indexFile;
    private readonly FileStream _contentFile;
    private  long _contentFileSize;
    private  long _indexFileSize;

    public IdValueStorage()
    {
        var indexFilePath = "nodes.index";
        var contentFilePath = "nodes.content";
        
        _indexFile = File.Open(indexFilePath, FileMode.OpenOrCreate);
        _contentFile = File.Open(contentFilePath, FileMode.OpenOrCreate);
        
        _contentFileSize = _contentFile.Seek(0, SeekOrigin.End);
        _indexFileSize = _indexFile.Seek(0, SeekOrigin.End);
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
        _indexFile.Seek(indexLocation, SeekOrigin.Begin);

        var contentPointerBuffer = new byte[8];
        var contentLengthBuffer = new byte[8];
        
        await _indexFile.ReadAsync(contentPointerBuffer, 0, 8);
        await _indexFile.ReadAsync(contentLengthBuffer, 0, 4);

        var contentPointer = BitConverter.ToInt64(contentPointerBuffer);
        var contentLength = BitConverter.ToInt32(contentLengthBuffer);

        var contentBuffer = new byte[contentLength];
        _contentFile.Seek(contentPointer, SeekOrigin.Begin);
        await _contentFile.ReadAsync(contentBuffer, 0, contentLength);

        return Encoding.Unicode.GetString(contentBuffer);
    }

    public void Dispose()
    {
        _indexFile.Dispose();
        _contentFile.Dispose();
    }
}