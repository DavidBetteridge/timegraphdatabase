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
        var dummyRow = new byte[]
        {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
        };
        await File.WriteAllBytesAsync(storage.BackingFilePath(), dummyRow);
        
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
        var expected = 
                // Dummy first row
                dummyRow
                
                // New row
                .Concat(BitConverter.GetBytes(10000L))
                .Concat(BitConverter.GetBytes(10001L))
                .Concat(BitConverter.GetBytes(10002L))
                .Concat(BitConverter.GetBytes(10003L));
        actual.Should().BeEquivalentTo(expected);
    }
    
    [Fact]
    public async Task BlocksOfTenRowsAreBrokenUpByFillerRows()
    {
        var storage = new Storage
        {
            FillFactor = 10
        };

        // Given an empty database
        File.Delete(storage.BackingFilePath());

        // Given the database contains 10 rows
        for (ulong i = 1; i < 11; i++)
        {
            await storage.InsertRowAsync(new StorageRecord
            {
                Timestamp = i,
                LhsId = i,
                RhsId = i,
                RelationshipId = i
            }); 
        }
        
        // When the 11th and 12th rows are inserted
        await storage.InsertRowAsync(new StorageRecord
        {
            Timestamp = 11,
            LhsId = 11,
            RhsId = 11,
            RelationshipId = 11
        });
        await storage.InsertRowAsync(new StorageRecord
        {
            Timestamp = 12,
            LhsId = 12,
            RhsId = 12,
            RelationshipId = 12
        });
        
        // Then the file contains rows 1-10,  A Filler,  then Row 11, and then Row 12
        var actual = await File.ReadAllBytesAsync(storage.BackingFilePath());
        var expected =
            AsRow(1,1,1,1)
                .Concat(AsRow(2,2,2,2))
                .Concat(AsRow(3,3,3,3))
                .Concat(AsRow(4,4,4,4))
                .Concat(AsRow(5,5,5,5))
                .Concat(AsRow(6,6,6,6))
                .Concat(AsRow(7,7,7,7))
                .Concat(AsRow(8,8,8,8))
                .Concat(AsRow(9,9,9,9))
                .Concat(AsRow(10,10,10,10))
                .Concat(AsRow(0,0,0,0))   // Filler
                .Concat(AsRow(11,11,11,11))
                .Concat(AsRow(12,12,12,12));
        actual.Should().BeEquivalentTo(expected);
    }

    private static IEnumerable<byte> AsRow(ulong i, ulong i1, ulong i2, ulong i3)
    {
        return BitConverter.GetBytes(i)
                .Concat(BitConverter.GetBytes(i1))
                .Concat(BitConverter.GetBytes(i2))
                .Concat(BitConverter.GetBytes(i3));
    }
}