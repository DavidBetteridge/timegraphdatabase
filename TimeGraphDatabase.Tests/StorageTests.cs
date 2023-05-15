using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class StorageTests
{
    [Fact]
    public async Task TheFirstRelationshipIsWrittenToTheFile()
    {
        var storage = new Storage();
        
        // Given no file currently exists
        File.Delete(storage.BackingFilePath());
        
        var record = new StorageRecord
        {
            Timestamp = 10000,
            LhsId = 10001,
            RhsId = 10002,
            RelationshipId = 10003
        };
        await storage.InsertRowAsync(record);

        // Entries are in the format:  Timestamp LhsId RhsId RelationshipId
        // ie.  each entry is 4 longs (4x8 bytes)
        var actual = await File.ReadAllBytesAsync(storage.BackingFilePath());
        var expected = BitConverter.GetBytes(10000L)
               .Concat(BitConverter.GetBytes(10001L))
               .Concat(BitConverter.GetBytes(10002L))
               .Concat(BitConverter.GetBytes(10003L));
        actual.Should().BeEquivalentTo(expected);
    }
    
    [Fact]
    public async Task TheMostRecentRelationshipIsAddedToTheEndOfTheFile()
    {
        var storage = new Storage();

        // Given a file already exists which contains all 1s for the first row.
        File.Delete(storage.BackingFilePath());
        await File.WriteAllBytesAsync(storage.BackingFilePath(), new byte[]
        {
            0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
            0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
            0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
            0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF
        });
        
        var record = new StorageRecord
        {
            Timestamp = 10000,
            LhsId = 10001,
            RhsId = 10002,
            RelationshipId = 10003
        };
        await storage.InsertRowAsync(record);

        // Entries are in the format:  Timestamp LhsId RhsId RelationshipId
        // ie.  each entry is 4 longs (4x8 bytes)
        var actual = await File.ReadAllBytesAsync(storage.BackingFilePath());
        var expected = BitConverter.GetBytes(10000L)
            .Concat(BitConverter.GetBytes(10001L))
            .Concat(BitConverter.GetBytes(10002L))
            .Concat(BitConverter.GetBytes(10003L));
        actual.Should().BeEquivalentTo(expected);
    }
}