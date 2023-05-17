using System.Collections;

namespace TimeGraphDatabase.Engine;

public class Storage : IDisposable
{
    private readonly FileStream _file;
    private int _numberOfRows;
    private const int BytesPerRow = 8 + 4 + 4 + 4;
    public int FillFactor { get; init; } = 10;
    private readonly byte[] _buffer = new byte[BytesPerRow];
    public Storage()
    {
        if (File.Exists(BackingFilePath()))
        {
            var length = new FileInfo(BackingFilePath()).Length;
            _numberOfRows = (int)(length / BytesPerRow);
        }
        
        _file = File.Open(BackingFilePath(), FileMode.OpenOrCreate);
    }
    
    public async ValueTask InsertRowAsync(StorageRecord record)
    {
        // Before we can insert our row, we need to check if a filler row is required.
        // This is the case where we contain at least FillFactor rows,  and none of the
        // last FillFactor rows are fillers.
        // TODO: Keep track of last fill factor
        var fillerNeeded = false;

        if (_numberOfRows >= FillFactor)
        {

            fillerNeeded = true;
            _file.Seek(-FillFactor * BytesPerRow, SeekOrigin.End);
            for (var lookBack = 1; lookBack <= FillFactor; lookBack++)
            {
                _file.Read(_buffer, 0, BytesPerRow);
                if (BufferContainsFiller())
                {
                    fillerNeeded = false;
                    break;
                }
            }
        }

        if (fillerNeeded)
        {
            // Write the filler
            _file.Seek(0, SeekOrigin.End);
            await WriteFillerAtCurrentLocation();
            _numberOfRows++;
        }
  
        _file.Seek(0, SeekOrigin.End);
        await _file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
        await _file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 4));
        _numberOfRows++;
    }

    private async Task WriteFillerAtCurrentLocation()
    {
        await _file.WriteAsync(BitConverter.GetBytes(0L).AsMemory(0, 8));
        await _file.WriteAsync(BitConverter.GetBytes(0).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(0).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(0).AsMemory(0, 4));
    }

    private bool BufferContainsFiller()
    {
        return _buffer[0] == 0x0 &&
               _buffer[1] == 0x0 &&
               _buffer[2] == 0x0 &&
               _buffer[3] == 0x0 &&
               _buffer[4] == 0x0 &&
               _buffer[5] == 0x0 &&
               _buffer[6] == 0x0 &&
               _buffer[7] == 0x0;
    }

    private void ReadRowIntoBuffer(int rowNumber)
    {
        _file.Seek(rowNumber * BytesPerRow, SeekOrigin.Begin);
        _file.Read(_buffer, 0, BytesPerRow);
    }
    
    public async Task DeleteRowAsync(StorageRecord storageRecord)
    {
        var toDelete = BitConverter.GetBytes(storageRecord.Timestamp)
            .Concat(BitConverter.GetBytes(storageRecord.LhsId))
            .Concat(BitConverter.GetBytes(storageRecord.RhsId))
            .Concat(BitConverter.GetBytes(storageRecord.RelationshipId)).ToArray();
        
        // Find the row to delete using a binary search
        var L = 0;
        var R = _numberOfRows - 1;
        while (L != R)
        {
            var m = (int)Math.Ceiling((L + R) / 2.0);
            ReadRowIntoBuffer(m);
            
            while (BufferContainsFiller() && m < R)
            {
                m++;
                ReadRowIntoBuffer(m);
            }

            if (BufferContainsFiller())
            {
                m = (int)Math.Ceiling((L + R) / 2.0);
                ReadRowIntoBuffer(m);
                while (BufferContainsFiller() && m > L)
                {
                    m--;
                    ReadRowIntoBuffer(m);
                }
            }

            if (BufferContainsFiller())
                return;
            
            
            if (_buffer.Compare(toDelete) > 0)
            {
                R = m - 1;
            }
            else
            {
                L = m;
            }
        }
        
        ReadRowIntoBuffer(L);
        if (_buffer.Compare(toDelete) == 0)
        {
            // Found
            _file.Seek(L * BytesPerRow, SeekOrigin.Begin);
            await WriteFillerAtCurrentLocation();
        }
        else
        {
            // Not found
        }
    }

    public static string BackingFilePath()
    {
        return "database.graph";
    }


    public void Dispose()
    {
        _file.Dispose();
    }
}

static class ArrayExtensions {
    public static int Compare(this byte[] b1, byte[] b2) {
        if (b1 == null && b2 == null)
            return 0;
        else if (b1 == null)
            return -1;
        else if (b2 == null)
            return 1;
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default);
    }
}