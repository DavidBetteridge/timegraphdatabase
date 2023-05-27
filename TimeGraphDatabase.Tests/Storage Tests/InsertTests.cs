using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class InsertTests : BaseStorageTest
{
    private const uint FILLER = 0;

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
        await GivenAFileContaining(1,2,3,4,5,6,7,8,9,10);
        
        await WhenTheRecordIsInserted(11);
        await WhenTheRecordIsInserted(12);

        await ThenTheFileMustContain(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, FILLER, 11, 12);
    }

    [Fact]
    public async Task RowsCanBeInsertedAtAFillerLocation()
    {
        await GivenAFileContaining(1, FILLER, 3);

        await WhenTheRecordIsInserted(2);

        await ThenTheFileMustContain(1, 2, 3);
    }


    [Fact]
    public async Task RowWillBeInsertedAndUseNextFiller()
    {
        await GivenAFileContaining(1, 2, 3, 4, 5, 6, 8, FILLER, 10);

        await WhenTheRecordIsInserted(7);

        await ThenTheFileMustContain(1, 2, 3, 4, 5, 6, 7, 8, 10);
    }

    [Fact]
    public async Task RowWillBeInsertedAndAddAFiller()
    {
        await GivenAFileContaining(1, 2, 3, 4, 5, 6, 8, 10);

        await WhenTheRecordIsInserted(7);

        await ThenTheFileMustContain(1, 2, 3, 4, 5, 6, 7, 8, 10);
    }

    [Fact]
    public async Task RowInsertedAtStart()
    {
        await GivenAFileContaining(2, 3, 4, 5, 6, 8, FILLER, 10);

        await WhenTheRecordIsInserted(1);

        await ThenTheFileMustContain(1, 2, 3, 4, 5, 6, 8, 10);
    }

    [Fact]
    public async Task RowInsertedAtStartReplacingFiller()
    {
        await GivenAFileContaining(FILLER, 2, 3, 4, 5, 6, 8, FILLER, 10);

        await WhenTheRecordIsInserted(1);

        await ThenTheFileMustContain(1, 2, 3, 4, 5, 6, 8, FILLER, 10);
    }

    private static async Task GivenAFileContaining(params uint[] rows)
    {
        foreach (var row in rows)
        {
            await InsertAtEndOfTestFile(row);
        }
    }
    
    private static async Task WhenTheRecordIsInserted(uint value)
    {
        using var storage = new Storage { FillFactor = 10 };
        await storage.InsertRowAsync(new StorageRecord
        {
            Timestamp = value,
            LhsId = value,
            RhsId = value,
            RelationshipId = value
        });
    }

    private static async Task ThenTheFileMustContain(params uint[] rows)
    {
        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());
        var expected = IsRow(rows[0]);
        foreach (var row in rows[1..])
        {
            expected = expected.Concat(IsRow(row));
        }

        actual.Should().BeEquivalentTo(expected);
    }



    // TEST:  Duplicate
}