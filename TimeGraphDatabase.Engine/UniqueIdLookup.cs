using System.Buffers.Binary;
using System.Diagnostics;

namespace TimeGraphDatabase.Engine;

public class UniqueIdLookup
{
    private readonly FileStream _file;
    
    private readonly byte[] _nodePointerBuffer = new byte[4];
    private byte[] _linkedListPointerBuffer = new byte[4];
    private long EndOfFile = 0;

    public UniqueIdLookup()
    {
        EndOfFile = 0;  //TODO
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
                locationOfList = BitConverter.ToInt32(_linkedListPointerBuffer);
                listSize *= 2;
            }
            else
            {
                _file.Seek(-4, SeekOrigin.Current); 
                await _file.WriteAsync(listSize); 
                
                // We need to create a list at the end of the file
                locationOfList = _file.Seek(0, SeekOrigin.End); 
                Debug.Assert(locationOfList==listSize);
                
                await _file.WriteAsync(nodeIsAsBytes); 
                listSize *= 2;
                for (int gapNumber = 1; gapNumber < listSize; gapNumber++)
                    await _file.WriteAsync(IntToBytes(0)); 
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

    private static byte[] LongToBytes(long value)
    {
        if (BitConverter.IsLittleEndian)
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(value));
        else
            return BitConverter.GetBytes(value);
    }
    
    private bool BufferIsZero(byte[] buffer)
    {
        return buffer[0] == 0x0 &&
               buffer[1] == 0x0 &&
               buffer[2] == 0x0 &&
               buffer[3] == 0x0 &&
               buffer[4] == 0x0 &&
               buffer[5] == 0x0 &&
               buffer[6] == 0x0 &&
               buffer[7] == 0x0;
    }
}