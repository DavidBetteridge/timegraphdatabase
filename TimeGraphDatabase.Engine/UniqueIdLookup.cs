using System.Buffers.Binary;
using System.Diagnostics;

namespace TimeGraphDatabase.Engine;

public class UniqueIdLookup : IDisposable
{
    private readonly FileStream _file;
    
    private readonly byte[] _nodePointerBuffer = new byte[4];
    private readonly byte[] _linkedListPointerBuffer = new byte[4];
    private int _endOfFile = 0;
    private int _numberOfBuckets = 1000;
    
    public UniqueIdLookup()
    {
        File.Delete(BackingFilePath());
        if (File.Exists(BackingFilePath()))
            _endOfFile = (int) new FileInfo(BackingFilePath()).Length;
        else
        {
            using FileStream fileStream = new FileStream(BackingFilePath(), FileMode.Create);
            byte[] buffer = new byte[1000 * 8]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file
            _endOfFile = 1000 * 8;
        }
        _file = File.Open(BackingFilePath(), FileMode.OpenOrCreate);
    }
    
    public static string BackingFilePath()
    {
        return "unique.index";
    }

    public async IAsyncEnumerable<int> FindAsync(string uniqueId)
    {
        var rowNumber = Math.Abs(uniqueId.GetHashCode()) % _numberOfBuckets;
        var listSize = 1; // The first list has a single space plus a pointer to the next list.
        var rowLength = (listSize * 4) + 4; 
        int locationOfList = rowNumber * rowLength;

        while (true)
        {
            // Move to the start of the list
            if (_file.Position != locationOfList)
                _file.Seek(locationOfList, SeekOrigin.Begin);
            
            // Check all the values in this list
            for (var gapNumber = 0; gapNumber < listSize; gapNumber++)
            {
                await _file.ReadAsync(_nodePointerBuffer.AsMemory(0, 4));
                if (!BufferIsZero(_nodePointerBuffer))
                {
                    locationOfList = BytesToInt(_nodePointerBuffer);
                    yield return locationOfList;
                }
            }

            // Node pointer
            await _file.ReadAsync(_linkedListPointerBuffer.AsMemory(0, 4)); // Linked list
            if (BufferIsZero(_linkedListPointerBuffer)) break;
            
            // Move to the next list
            locationOfList = BytesToInt(_linkedListPointerBuffer);
            listSize *= 2;
        }
    }

    public async Task UpdateAsync(int nodeId, string originalUniqueId, string replacementUniqueId)
    {
        var originalRowNumber = Math.Abs(originalUniqueId.GetHashCode()) % _numberOfBuckets;
        var replacementRowNumber = Math.Abs(replacementUniqueId.GetHashCode()) % _numberOfBuckets;
        if (originalRowNumber == replacementRowNumber) return;  // They both hash to the same thing,  no update needed

        await DeleteAsync(nodeId, originalUniqueId);
        await InsertAsync(nodeId, replacementUniqueId);
    }
    
    public async Task<bool> DeleteAsync(int nodeId, string uniqueId)
    {
        var rowNumber = Math.Abs(uniqueId.GetHashCode()) % _numberOfBuckets;
        var listSize = 1; // The first list has a single space plus a pointer to the next list.
        var rowLength = (listSize * 4) + 4; 
        int locationOfList = rowNumber * rowLength;

        while (true)
        {
            // Move to the start of the list
            if (_file.Position != locationOfList)
                _file.Seek(locationOfList, SeekOrigin.Begin);
            
            // Check all the values in this list
            for (var gapNumber = 0; gapNumber < listSize; gapNumber++)
            {
                await _file.ReadAsync(_nodePointerBuffer.AsMemory(0, 4));
                if (!BufferIsZero(_nodePointerBuffer))
                {
                    locationOfList = BytesToInt(_nodePointerBuffer);
                    if (nodeId == locationOfList)
                    {
                        // We have found our entry
                        _file.Seek(-4, SeekOrigin.Current); 
                        await _file.WriteAsync(IntToBytes(0));
                        return true;
                    }
                }
            }

            // Node pointer
            await _file.ReadAsync(_linkedListPointerBuffer.AsMemory(0, 4)); // Linked list
            if (BufferIsZero(_linkedListPointerBuffer)) return false;
            
            // Move to the next list
            locationOfList = BytesToInt(_linkedListPointerBuffer);
            listSize *= 2;
        }
    }

    public async Task InsertAsync(int nodeId, string uniqueId)
    {
        byte[] nodeIsAsBytes = IntToBytes(nodeId);
        var rowNumber = Math.Abs(uniqueId.GetHashCode()) % _numberOfBuckets;
        
        // Find the first list which has a gap.  If no lists have any gaps
        // then we have to add a new list to the end of the file.
        
        var listSize = 1; // The first list has a single space plus a pointer to the next list.
        var rowLength = 4 + 4; 
        long locationOfList = rowNumber * rowLength;

        while (true)
        {
            _file.Seek(locationOfList, SeekOrigin.Begin);
            for (int gapNumber = 0; gapNumber < listSize; gapNumber++)
            {
                await _file.ReadAsync(_nodePointerBuffer.AsMemory(0, 4));
                if (BufferIsZero(_nodePointerBuffer))
                {
                    _file.Seek(-4, SeekOrigin.Current); 
                    await _file.WriteAsync(nodeIsAsBytes);   // Int  (4 bytes)
                    return;
                }
            }
            
            // Node pointer
            await _file.ReadAsync(_linkedListPointerBuffer.AsMemory(0, 4));    // Linked list
            if (!BufferIsZero(_linkedListPointerBuffer))
            {
                // We have a pointer to the next list
                locationOfList = BytesToInt(_linkedListPointerBuffer);
                listSize *= 2;
            }
            else
            {
                // We need to create a new list
                
                // Write the location of the next list (which is the end of the file)
                _file.Seek(-4, SeekOrigin.Current); 
                await _file.WriteAsync(IntToBytes(_endOfFile)); 
                
                // We need to create a list at the end of the file
                locationOfList = _file.Seek(0, SeekOrigin.End); 
                Debug.Assert(locationOfList==_endOfFile);
                
                // Write our node to the file
                await _file.WriteAsync(nodeIsAsBytes); 

                // Write the rest of the entries as zeros
                listSize *= 2;
                for (int gapNumber = 1; gapNumber < listSize; gapNumber++)
                    await _file.WriteAsync(IntToBytes(0)); 
                
                // Write zero for the location of the following list.
                await _file.WriteAsync(IntToBytes(0));
                _endOfFile += (listSize * 4) + 4;
                return;
            }
        }
    }

    private static byte[] IntToBytes(int value)
    {
        if (BitConverter.IsLittleEndian)
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(value));
        else
            return BitConverter.GetBytes(value);
    }

    private static int BytesToInt(byte[] value)
    {
        if (BitConverter.IsLittleEndian)
            return  BinaryPrimitives.ReverseEndianness(BitConverter.ToInt32(value));
        else
            return BitConverter.ToInt32(value);
    }
    
    private bool BufferIsZero(byte[] buffer)
    {
        return buffer[0] == 0x0 &&
               buffer[1] == 0x0 &&
               buffer[2] == 0x0 &&
               buffer[3] == 0x0;
    }

    public void Dispose()
    {
        _file.Dispose();
    }
}