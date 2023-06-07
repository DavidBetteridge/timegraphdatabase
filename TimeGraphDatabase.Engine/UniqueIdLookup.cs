using System.Buffers.Binary;
using System.Diagnostics;

namespace TimeGraphDatabase.Engine;

public class UniqueIdLookup : IDisposable
{
    private readonly FileStream _file;
    
    private readonly byte[] _nodePointerBuffer = new byte[4];
    private readonly byte[] _linkedListPointerBuffer = new byte[4];
    private int EndOfFile = 0;

    public UniqueIdLookup()
    {
        if (File.Exists(BackingFilePath()))
            EndOfFile = (int) new FileInfo(BackingFilePath()).Length;
        else
        {
            using FileStream fileStream = new FileStream(BackingFilePath(), FileMode.Create);
            byte[] buffer = new byte[1000 * 8]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file
        }
        _file = File.Open(BackingFilePath(), FileMode.OpenOrCreate);
    }
    
    public static string BackingFilePath()
    {
        return "unique.index";
    }
    
    public async Task InsertAsync(int nodeId, string uniqueId)
    {
        byte[] nodeIsAsBytes = IntToBytes(nodeId);
        
        var numberOfBuckets = 1000;
        var rowNumber = uniqueId.GetHashCode() % numberOfBuckets;
        
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
                await _file.WriteAsync(IntToBytes(EndOfFile)); 
                
                // We need to create a list at the end of the file
                locationOfList = _file.Seek(0, SeekOrigin.End); 
                Debug.Assert(locationOfList==listSize);
                
                // Write our node to the file
                await _file.WriteAsync(nodeIsAsBytes); 

                // Write the rest of the entries as zeros
                listSize *= 2;
                for (int gapNumber = 1; gapNumber < listSize; gapNumber++)
                    await _file.WriteAsync(IntToBytes(0)); 
                
                // Write zero for the location of the following list.
                await _file.WriteAsync(IntToBytes(0));
                EndOfFile += (listSize * 4) + 4;
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