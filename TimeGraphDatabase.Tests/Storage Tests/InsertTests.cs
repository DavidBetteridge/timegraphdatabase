using System.Buffers.Binary;
using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class InsertTests : BaseStorageTest
{
    private const uint FILLER = 0;

    [Fact]
    public async Task TheFirstRelationshipIsWrittenToTheFile()
    {
        await WhenTheRecordIsInserted(10000);
        await ThenTheFileMustContain(10000);
    }

    [Fact]
    public async Task TheMostRecentRelationshipIsAddedToTheEndOfTheFile()
    {
        await GivenAFileContaining(1);

        await WhenTheRecordIsInserted(10000);
        
        await ThenTheFileMustContain(1, 10000);
    }

    [Fact]
    public async Task SimpleTest()
    {
        await GivenAFileContaining(1, 2, 3);

        await WhenTheRecordIsInserted(4);

        await ThenTheFileMustContain(1, 2, 3, 4);
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

    private static async Task ThenTheFileMustContain(params ulong[] rows)
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