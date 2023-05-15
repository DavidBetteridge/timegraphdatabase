using System;

namespace TimeGraphDatabase.Engine;

public class Storage
{
    public async ValueTask InsertRowAsync(StorageRecord record)
    {
        await using var file = File.Open(BackingFilePath(), FileMode.OpenOrCreate);
        file.Seek(0, SeekOrigin.End);
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