namespace TimeGraphDatabase.Engine;

public class Storage
{
    private const int BytesPerRow = 8 * 4;  //ulongs or uints?
    public int FillFactor { get; init; } = 10;
    
    public async ValueTask InsertRowAsync(StorageRecord record)
    {
        await using var file = File.Open(BackingFilePath(), FileMode.OpenOrCreate);  //TODO:  move up into class var
        
        
        // Before we can insert our row, we need to check if a filler row is required.
        // This is the case where we contain at least FillFactor rows,  and none of the
        // last FillFactor rows are fillers.
        // TODO: Keep track of last fill factor
        var length = new FileInfo(BackingFilePath()).Length;
        var numberOfRows = (int)(length / BytesPerRow);
        var fillerNeeded = false;

        if (numberOfRows >= FillFactor)
        {
            var buffer = new byte[BytesPerRow];  //TODO:  move up into class var
            fillerNeeded = true;
            file.Seek(-FillFactor * BytesPerRow, SeekOrigin.End);
            for (var lookBack = 1; lookBack <= FillFactor; lookBack++)
            {
                file.Read(buffer, 0, BytesPerRow);
                if (buffer[0] == 0x0 && 
                    buffer[1] == 0x0 &&
                    buffer[2] == 0x0 &&
                    buffer[3] == 0x0 &&
                    buffer[4] == 0x0 &&
                    buffer[5] == 0x0 &&
                    buffer[6] == 0x0 &&
                    buffer[7] == 0x0)
                {
                    fillerNeeded = false;
                    break;
                }
            }
        }
        
        file.Seek(0, SeekOrigin.End);
        if (fillerNeeded)
        {
            // Write the filler
            await file.WriteAsync(BitConverter.GetBytes(0L).AsMemory(0, 8));
            await file.WriteAsync(BitConverter.GetBytes(0L).AsMemory(0, 8));
            await file.WriteAsync(BitConverter.GetBytes(0L).AsMemory(0, 8));
            await file.WriteAsync(BitConverter.GetBytes(0L).AsMemory(0, 8));  
        }
        
        await file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
        await file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 8));
        await file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 8));
        await file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 8));
    }

    public string BackingFilePath()
    {
        return "database.graph";
    }

}