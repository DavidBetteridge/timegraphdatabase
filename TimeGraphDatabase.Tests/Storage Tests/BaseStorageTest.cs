using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

[Collection("storage")]
public abstract class BaseStorageTest
{
    protected BaseStorageTest()
    {
        // Given no file currently exists
        File.Delete(Storage.BackingFilePath());
    }
    
    protected static IEnumerable<byte> IsRow(ulong i, uint i1, uint i2, uint i3)
    {
        return BitConverter.GetBytes(i)
            .Concat(BitConverter.GetBytes(i1))
            .Concat(BitConverter.GetBytes(i2))
            .Concat(BitConverter.GetBytes(i3));
    }
    
    protected static IEnumerable<byte> IsRow(uint i)
    {
        return BitConverter.GetBytes((ulong)i)
            .Concat(BitConverter.GetBytes(i))
            .Concat(BitConverter.GetBytes(i))
            .Concat(BitConverter.GetBytes(i));
    }

    protected static IEnumerable<byte> IsFiller() => IsRow(0, 0, 0, 0);

    protected static async Task InsertAtEndOfTestFile(StorageRecord record)
    {
        await using var file = File.Open(Storage.BackingFilePath(), FileMode.OpenOrCreate);
        file.Seek(0, SeekOrigin.End);
        await file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
        await file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 4));
        await file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 4));
        await file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 4));
    }
    
    protected static Task InsertAtEndOfTestFile(uint value)
    {
        return InsertAtEndOfTestFile(new StorageRecord
        {
            Timestamp = value,
            LhsId = value,
            RhsId = value,
            RelationshipId = value,
        });
    }
}