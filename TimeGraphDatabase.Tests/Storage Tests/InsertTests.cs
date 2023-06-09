using System.Buffers.Binary;
using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class InsertTests : BaseStorageTest
{

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
    
    [Fact]
    public async Task RowsInsertedInReverseOrder()
    {
        await WhenTheRecordIsInserted(250000);
        await WhenTheRecordIsInserted(249999);
        await WhenTheRecordIsInserted(249998);
        await WhenTheRecordIsInserted(249997);

        await ThenTheFileMustContain(249997, 249998, 249999, 250000);
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