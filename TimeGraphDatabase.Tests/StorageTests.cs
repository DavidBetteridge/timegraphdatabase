using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class StorageTests
{
    public StorageTests()
    {
        // Given no file currently exists
        File.Delete(Storage.BackingFilePath()); 
    }
    
    [Fact]
    public async Task TheFirstRelationshipIsWrittenToTheFile()
    {
        var record = new StorageRecord
        {
            Timestamp = 10000L,
            LhsId = 10001,
            RhsId = 10002,
            RelationshipId = 10003
        };
        
        using (var storage = new Storage())
        {
            await storage.InsertRowAsync(record);
        }

        // Entries are in the format:  Timestamp LhsId RhsId RelationshipId
        // ie.  each entry is 4 longs (4x8 bytes)
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected = BitConverter.GetBytes(10000L)
               .Concat(BitConverter.GetBytes(10001))
               .Concat(BitConverter.GetBytes(10002))
               .Concat(BitConverter.GetBytes(10003));
        actual.Should().BeEquivalentTo(expected);
    }
    
    [Fact]
    public async Task TheMostRecentRelationshipIsAddedToTheEndOfTheFile()
    {
        // Given a file already exists which contains all 1s for the first row.
        var dummyRow = new byte[]
        {
            0xFF, 0xFF, 0xFF, 0xFF,0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        };
        await File.WriteAllBytesAsync(Storage.BackingFilePath(), dummyRow);
        
        var record = new StorageRecord
        {
            Timestamp = 10000L,
            LhsId = 10001,
            RhsId = 10002,
            RelationshipId = 10003
        };
        
        using (var storage = new Storage())
        {
            await storage.InsertRowAsync(record);
        }

        // Entries are in the format:  Timestamp LhsId RhsId RelationshipId
        // ie.  each entry is 4 longs (4x8 bytes)
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected = 
                // Dummy first row
                dummyRow
                
                // New row
                .Concat(BitConverter.GetBytes(10000L))
                .Concat(BitConverter.GetBytes(10001))
                .Concat(BitConverter.GetBytes(10002))
                .Concat(BitConverter.GetBytes(10003));
        actual.Should().BeEquivalentTo(expected);
    }
    
    [Fact]
    public async Task BlocksOfTenRowsAreBrokenUpByFillerRows()
    {
        using (var storage = new Storage { FillFactor = 10 })
        {
            // Given the database contains 10 rows
            for (uint i = 1; i < 11; i++)
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
        }

        // Then the file contains rows 1-10,  A Filler,  then Row 11, and then Row 12
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected =
            IsRow(1,1,1,1)
                .Concat(IsRow(2,2,2,2))
                .Concat(IsRow(3,3,3,3))
                .Concat(IsRow(4,4,4,4))
                .Concat(IsRow(5,5,5,5))
                .Concat(IsRow(6,6,6,6))
                .Concat(IsRow(7,7,7,7))
                .Concat(IsRow(8,8,8,8))
                .Concat(IsRow(9,9,9,9))
                .Concat(IsRow(10,10,10,10))
                .Concat(IsFiller())
                .Concat(IsRow(11,11,11,11))
                .Concat(IsRow(12,12,12,12));
        actual.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task DeletingARowReplacesItWithAFiller()
    {
        using (var storage = new Storage { FillFactor = 10 })
        {
            // Given the database contains 10 rows
            for (uint i = 1; i < 11; i++)
            {
                await storage.InsertRowAsync(new StorageRecord
                {
                    Timestamp = i,
                    LhsId = i,
                    RhsId = i,
                    RelationshipId = i
                });
            }

            await storage.DeleteRowAsync(new StorageRecord
            {
                Timestamp = 3,
                LhsId = 3,
                RhsId = 3,
                RelationshipId = 3
            });
        }

        // Then the file has the 3rd row replaced a filler
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected =
            IsRow(1,1,1,1)
                .Concat(IsRow(2,2,2,2))
                .Concat(IsFiller()) 
                .Concat(IsRow(4,4,4,4))
                .Concat(IsRow(5,5,5,5))
                .Concat(IsRow(6,6,6,6))
                .Concat(IsRow(7,7,7,7))
                .Concat(IsRow(8,8,8,8))
                .Concat(IsRow(9,9,9,9))
                .Concat(IsRow(10,10,10,10));
        actual.Should().BeEquivalentTo(expected);
        
        // DATA DRIVEN TEST WHERE FILLERS EXIST
    }
    
    private static IEnumerable<byte> IsRow(ulong i, uint i1, uint i2, uint i3)
    {
        return BitConverter.GetBytes(i)
                .Concat(BitConverter.GetBytes(i1))
                .Concat(BitConverter.GetBytes(i2))
                .Concat(BitConverter.GetBytes(i3));
    }

    private static IEnumerable<byte> IsFiller() => IsRow(0, 0, 0, 0);
}


/*
 * INSERT TESTS
 * Rows can be appended    ✅  
 * Fillers are inserted at the end of the file  ✅
 * Row can be inserted if lines up with a filler
 * Row can be inserted by moving rows forward/backwards to the next filler
 * Rows can be split into multiple file with they become full.
 *
 * DELETE TESTS  
 * When rows are deleted they are replaced by a filler
 *
 *
 * QUERY TESTS
 * All entries in a single file < X
 * All entries from split files < X
 * Entries with predicate ( lhs, rhs, relation => bool )
 */