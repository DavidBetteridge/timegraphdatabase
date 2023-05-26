using System.Collections;

namespace TimeGraphDatabase.Engine;

public class Storage : IDisposable
{
    private readonly FileStream _file;
    private int _numberOfRows;
    public static readonly int BytesPerRow = 8 + 4 + 4 + 4;
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
        var toInsert = record.ToByteArray();
        
        //////////////////////////////////////////////////////////////
        // Simple case - an empty file.
        if (_numberOfRows == 0)
        {
            await InsertRecordAtEndOfFile(record);
            return;
        }
        
        //////////////////////////////////////////////////////////////
        // Preferred case, inserting a row at the end of the file.
        
        // Work back from the end of the file to find the first row which doesn't contain a filler.
        var lastNoneFillerRowNumber = _numberOfRows - 1;
        ReadRowIntoBuffer(lastNoneFillerRowNumber);
        while (lastNoneFillerRowNumber >= 0 && BufferContainsFiller())
        {
            lastNoneFillerRowNumber--;
            ReadRowIntoBuffer(lastNoneFillerRowNumber);
        }

        if (lastNoneFillerRowNumber == -1)
        {
            // The file only contains fillers.  So we can write our record anywhere.
            OverwriteRow(record, 0);
            return;
        }
        
        if (_buffer.LessThan(toInsert))
        {
            // The last value in the file comes before ours.
            if (lastNoneFillerRowNumber == _numberOfRows - 1)
            {
                // We need to add a row to the end of the file
                await InsertRecordAtEndOfFile(record);
                return;
            }
            else
            {
                // Replace the first filler after the final populated row with our row.
                OverwriteRow(record, lastNoneFillerRowNumber+1);
                return;
            }
        }
        
        
        //////////////////////////////////////////////////////////////
        // Now we have to insert the value mid-file.  Find the location
        // using a binary search,  allowing for fillers.

        // We know there exists a value in the file which is bigger than I. Now we need to 
        // find 'm' such that X[m-1] < I and X[m] >= I.
        
        var L = 0;
        var R = _numberOfRows - 1;
        var solutionFound = false;
        while (L < R && !solutionFound)
        {
            solutionFound = true;  // We need to prove this isn't the correct location.
            var m = (int)Math.Ceiling((L + R) / 2.0);
        
            // Find the first value to the right which isn't a filler.
            var mUpper = m;
            ReadRowIntoBuffer(mUpper);
            while (BufferContainsFiller() && mUpper < R)
            {
                mUpper++;
                ReadRowIntoBuffer(mUpper);
            }

            if (_buffer.LessThan(toInsert))
            {
                // Our guess is too small.
                L = m;
                solutionFound = false;
            }
            
            // Find the first value to the left which isn't a filler.
            var mLower = m-1;
            ReadRowIntoBuffer(mLower);
            while (BufferContainsFiller() && mLower > L)
            {
                mLower--;
                ReadRowIntoBuffer(mLower);
            }

            if (_buffer.GreaterThanOrEqual(toInsert))
            {
                // Our guess is too large.
                R = m - 1;
                solutionFound = false;
            }
            
            if (solutionFound)
            {
                // Shuffle the closest filler into location 'm'
                
                // Find the first filler after 'm'
                mUpper = m;
                ReadRowIntoBuffer(mUpper);
                while (!BufferContainsFiller() && mUpper < (_numberOfRows - 1))
                {
                    mUpper++;
                    ReadRowIntoBuffer(mUpper);
                }

                if (!BufferContainsFiller() && mUpper == (_numberOfRows - 1))
                {
                    // We have reached the end of the file without finding a buffer.  So we append on to the end.
                    WriteFillerAtCurrentLocation();
                    mUpper++;
                }
                
                // We know have a filler at location mUpper,  which we need to shuffle back to location 'm'
                while (mUpper > m)
                {
                    ReadRowIntoBuffer(mUpper-1);
                    _file.Seek((mUpper) * BytesPerRow, SeekOrigin.Begin);
                    await _file.WriteAsync(_buffer);
                    mUpper--;
                }
                
                // Insert the row at location m
                await OverwriteRow(record, m);
                return;
            }
        }

    }

    private async Task OverwriteRow(StorageRecord record, int m)
    {
        _file.Seek(m * BytesPerRow, SeekOrigin.Begin);
        await _file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
        await _file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 4));
        await _file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 4));
    }

    private async Task InsertRecordAtEndOfFile(StorageRecord record)
    {
        // Before we can insert our row, we need to check if a filler row is required.
        // This is the case where we contain at least FillFactor rows,  and none of the
        // last FillFactor rows are fillers.

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
        
        // Now we can insert our actual record
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
        var toDelete = storageRecord.ToByteArray();
        
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
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default);
    }
    public static bool LessThan(this byte[] b1, byte[] b2) {
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default) < 0;
    }
    public static bool LessThanOrEqual(this byte[] b1, byte[] b2) {
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default) <= 0;
    }
    public static bool GreaterThan(this byte[] b1, byte[] b2) {
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default) > 0;
    }
    public static bool GreaterThanOrEqual(this byte[] b1, byte[] b2) {
        return ((IStructuralComparable) b1).CompareTo(b2, Comparer<byte>.Default) >= 0;
    }
}