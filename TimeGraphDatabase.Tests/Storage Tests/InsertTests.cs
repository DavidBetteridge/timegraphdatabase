using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class InsertTests : BaseStorageTest
{
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
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01,
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
            IsRow(1, 1, 1, 1)
                .Concat(IsRow(2, 2, 2, 2))
                .Concat(IsRow(3, 3, 3, 3))
                .Concat(IsRow(4, 4, 4, 4))
                .Concat(IsRow(5, 5, 5, 5))
                .Concat(IsRow(6, 6, 6, 6))
                .Concat(IsRow(7, 7, 7, 7))
                .Concat(IsRow(8, 8, 8, 8))
                .Concat(IsRow(9, 9, 9, 9))
                .Concat(IsRow(10, 10, 10, 10))
                .Concat(IsFiller())
                .Concat(IsRow(11, 11, 11, 11))
                .Concat(IsRow(12, 12, 12, 12));
        actual.Should().BeEquivalentTo(expected);
    }
    
    [Fact]
    public async Task RowsCanBeInsertedAtAFillerLocation()
    {
        // Given a file containing three rows:  1, FILLER, 3
        await InsertAtEndOfTestFile(1);
        await InsertAtEndOfTestFile(0);
        await InsertAtEndOfTestFile(3);

        // When a row at time 2 is inserted
        using (var storage = new Storage { FillFactor = 10 })
        {
            await storage.InsertRowAsync(new StorageRecord
            {
                Timestamp = 2,
                LhsId = 2,
                RhsId = 2,
                RelationshipId = 2
            });
        }

        // Then the file contains 1, 2, 3
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected =
            IsRow(1)
                .Concat(IsRow(2))
                .Concat(IsRow(3));
        actual.Should().BeEquivalentTo(expected);
    }

    
    [Fact]
    public async Task RowWillBeInsertedAndUseNextFiller()
    {
        // Given a file containing these rows:  1, 2, 3, 4, 5, 6, 8, FILLER, 10
        await InsertAtEndOfTestFile(1);
        await InsertAtEndOfTestFile(2);
        await InsertAtEndOfTestFile(3);
        await InsertAtEndOfTestFile(4);
        await InsertAtEndOfTestFile(5);
        await InsertAtEndOfTestFile(6); 
        await InsertAtEndOfTestFile(8);
        await InsertAtEndOfTestFile(0);  //FILLER
        await InsertAtEndOfTestFile(10);

        // When row 7 is inserted
        using (var storage = new Storage { FillFactor = 10 })
        {
            await storage.InsertRowAsync(new StorageRecord
            {
                Timestamp = 7,
                LhsId = 7,
                RhsId = 7,
                RelationshipId = 7
            });
        }

        // Then the file contains 1, 2, 3, 4, 5, 6, 7, 8, 10
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected =
                        IsRow(1)
                .Concat(IsRow(2))
                .Concat(IsRow(3))
                .Concat(IsRow(4))
                .Concat(IsRow(5))
                .Concat(IsRow(6))
                .Concat(IsRow(7))
                .Concat(IsRow(8))
                .Concat(IsRow(10))
            ;
        actual.Should().BeEquivalentTo(expected);
    }

    // TEST:  Insert at start of file
}