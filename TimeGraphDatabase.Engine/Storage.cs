using System.Collections;

namespace TimeGraphDatabase.Engine;

public class Storage : IDisposable
{
    private readonly FileStream _file;
    private int _numberOfRows;
    public static int BytesPerRow = 8 + 4 + 4 + 4;
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

    public async ValueTask DefragAsync()
    {
        // How many fillers to we currently have?
        var numberOfFillers = 0;
        for (int i = 0; i < _numberOfRows; i++)
        {
            ReadRowIntoBuffer(i);
            if (BufferContainsFiller())
                numberOfFillers++;
        }
        
        // How many should we have?
        var idealNumberOfFillers = numberOfFillers / FillFactor;
        var numberMissing = idealNumberOfFillers - numberOfFillers;

        if (numberMissing>0)
            await DefragFromEndOfFile(numberMissing);
        else
            await DefragFromStartOfFile();
    }

    private async Task DefragFromStartOfFile()
    {
        
        var nextRead = 0;
        var nextWrite = 0;

        while (nextWrite < _numberOfRows)
        {
            if (nextRead > _numberOfRows) nextRead = _numberOfRows;

            // Advance nextRead until we find a value which isn't a filler.
            ReadRowIntoBuffer(nextRead);
            while (nextRead < _numberOfRows && BufferContainsFiller())
            {
                nextRead++;
                ReadRowIntoBuffer(nextRead);
            }

            ReadRowIntoBuffer(nextWrite);
            var currentWriteIsFiller = BufferContainsFiller();
            var currentWriteShouldBeAFiller = nextWrite % (FillFactor + 1) == FillFactor;

            if (currentWriteIsFiller && currentWriteShouldBeAFiller)
            {
                // All good
                nextWrite++;
            }
            else if (!currentWriteShouldBeAFiller)
            {
                // Write contents of nextRead to nextWrite
                ReadRowIntoBuffer(nextRead);
                _file.Seek(nextWrite * BytesPerRow, SeekOrigin.Begin);
                await _file.WriteAsync(_buffer);
                
                _file.Seek(nextRead * BytesPerRow, SeekOrigin.Begin);
                await WriteFillerAtCurrentLocation();

                nextRead++;
                nextWrite++;
            }
            else if (!currentWriteIsFiller && currentWriteShouldBeAFiller)
            {
                // Write filler to nextWrite
                _file.Seek(nextWrite * BytesPerRow, SeekOrigin.Begin);
                await WriteFillerAtCurrentLocation();
                nextWrite++;
            }
        }
        
    }

    private async Task DefragFromEndOfFile(int numberMissing)
    {
        // Add the missing fillers to the end of our file.
        while (numberMissing > 0)
        {
            _file.Seek(0, SeekOrigin.End);
            await WriteFillerAtCurrentLocation();
            _numberOfRows++;
            numberMissing--;
        }

        if (numberMissing >= 0)
        {
            var nextRead = _numberOfRows;
            var nextWrite = _numberOfRows;

            while (nextWrite >= 0)
            {
                if (nextRead < 0) nextRead = 0;

                // Advance nextRead until we find a value which isn't a filler.
                ReadRowIntoBuffer(nextRead);
                while (nextRead > 0 && BufferContainsFiller())
                {
                    nextRead--;
                    ReadRowIntoBuffer(nextRead);
                }

                ReadRowIntoBuffer(nextWrite);
                var currentWriteIsFiller = BufferContainsFiller();
                var currentWriteShouldBeAFiller = nextWrite % (FillFactor + 1) == FillFactor;

                if (currentWriteIsFiller && currentWriteShouldBeAFiller)
                {
                    // All good
                    nextWrite--;
                }
                else if (!currentWriteShouldBeAFiller)
                {
                    // Write contents of nextRead to nextWrite
                    ReadRowIntoBuffer(nextRead);
                    _file.Seek(nextWrite * BytesPerRow, SeekOrigin.Begin);
                    await _file.WriteAsync(_buffer);

                    nextRead--;
                    nextWrite--;
                }
                else if (!currentWriteIsFiller && currentWriteShouldBeAFiller)
                {
                    // Write filler to nextWrite
                    _file.Seek(nextWrite * BytesPerRow, SeekOrigin.Begin);
                    await WriteFillerAtCurrentLocation();
                    nextWrite--;
                }
            }
        }
    }

    public async ValueTask InsertRowAsync(StorageRecord record)
    {
        // Work out the location to insert our row.  We do a binary search until we
        // a.  Find that we are the earliest entry in the file X < A[0]
        // b.  Find that we are the last entry in the file  X > A[n]
        // c.  Find the location in the file with  A[n] < X < A[n+1] (fillers can exist between A[n] and A[n+1])
        // d.  We already exist in the file.  ie.  A[n] == X
        
        // For cases a and c,  we then shuffle the closest filler into the desired location. 
        // Replace the filler with our new row.
        // For b,  we add to the end of the file,  with a filler as required.
        // For d,  we exit
        
        
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
    
    public async Task<bool> DeleteRowAsync(StorageRecord storageRecord)
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
                while (BufferContainsFiller() && m > L)
                {
                    m--;
                    ReadRowIntoBuffer(m);
                }
            }

            if (BufferContainsFiller())
                return false;
            
            
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
            return true;
        }
        else
        {
            // Not found
            return false;
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