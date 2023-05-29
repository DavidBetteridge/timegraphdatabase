using System.Buffers.Binary;
using System.ComponentModel.Design;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

[Collection("storage")]
public abstract class BaseStorageTest
{
    protected const uint FILLER = 0;

    protected BaseStorageTest()
    {
        // Given no file currently exists
        File.Delete(Storage.BackingFilePath());
    }
    
    protected static IEnumerable<byte> IsRow(ulong i, uint i1, uint i2, uint i3)
    {
        if (BitConverter.IsLittleEndian)
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(i))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(i1)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(i2)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(i3)));
        else
            return BitConverter.GetBytes(i)
                .Concat(BitConverter.GetBytes(i))
                .Concat(BitConverter.GetBytes(i))
                .Concat(BitConverter.GetBytes(i));
    }
    
    protected static IEnumerable<byte> IsRow(ulong i)
    {
        var timestamp = (i == 0) ? 0 : (ulong)new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(i)
            .ToUnixTimeMilliseconds();
        
        if (BitConverter.IsLittleEndian)
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(timestamp))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness((uint)i)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness((uint)i)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness((uint)i)));
        else
            return BitConverter.GetBytes(timestamp)
                .Concat(BitConverter.GetBytes((uint)i))
                .Concat(BitConverter.GetBytes((uint)i))
                .Concat(BitConverter.GetBytes((uint)i));
    }

    protected static IEnumerable<byte> IsFiller() => IsRow(0, 0, 0, 0);

    protected static async Task InsertAtEndOfTestFile(StorageRecord record)
    {
        await using var file = File.Open(Storage.BackingFilePath(), FileMode.OpenOrCreate);
        file.Seek(0, SeekOrigin.End);
        
        if (BitConverter.IsLittleEndian)
        {
            await file.WriteAsync(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(record.Timestamp))
                .AsMemory(0, 8));
            await file.WriteAsync(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(record.LhsId)).AsMemory(0, 4));
            await file.WriteAsync(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(record.RhsId)).AsMemory(0, 4));
            await file.WriteAsync(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(record.RelationshipId)).AsMemory(0, 4));
        }
        else
        {
            await file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
            await file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 4));
            await file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 4));
            await file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 4));
        }
        
    }
    
    protected static Task InsertAtEndOfTestFile(uint value)
    {
        return InsertAtEndOfTestFile(new StorageRecord
        {
            Timestamp = (value == 0) ? 0 : (ulong)new DateTimeOffset(2023,1,1,0,0,0, TimeSpan.Zero).AddDays(value).ToUnixTimeMilliseconds(),
            LhsId = value,
            RhsId = value,
            RelationshipId = value,
        });
    }
    
    protected static async Task WhenTheRecordIsInserted(uint value)
    {
        using var storage = new Storage { FillFactor = 10 };
        var timestamp = (value==FILLER) ? 0 : (ulong)new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(value)
            .ToUnixTimeMilliseconds();
        await storage.InsertRowAsync(new StorageRecord
        {
            Timestamp = timestamp,
            LhsId = value,
            RhsId = value,
            RelationshipId = value
        });
    }
    
    protected static async Task GivenAFileContaining(params uint[] rows)
    {
        foreach (var row in rows)
        {
            await InsertAtEndOfTestFile(row);
        }
    }
}