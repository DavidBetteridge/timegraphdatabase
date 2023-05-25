using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class DeleteTests : BaseStorageTest
{
    [Theory]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 1, new uint[] { 0, 2, 3, 4, 5, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 2, new uint[] { 1, 0, 3, 4, 5, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 3, new uint[] { 1, 2, 0, 4, 5, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 4, new uint[] { 1, 2, 3, 0, 5, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 5, new uint[] { 1, 2, 3, 4, 0, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 6, new uint[] { 1, 2, 3, 4, 5, 0, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 7, new uint[] { 1, 2, 3, 4, 5, 6, 0, 8, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 8, new uint[] { 1, 2, 3, 4, 5, 6, 7, 0, 9, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 9, new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 0, 10 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 10, new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 0, 6, 7, 8, 9, 10 }, 10, new uint[] { 1, 2, 3, 4, 0, 6, 7, 8, 9, 0 })]
    [InlineData(new uint[] { 1, 2, 3, 4, 0, 6, 7, 8, 9, 10 }, 11, new uint[] { 1, 2, 3, 4, 0, 6, 7, 8, 9, 10 })]
    [InlineData(new uint[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 11, new uint[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 })]
    public async Task DeletingARowReplacesItWithAFiller(uint[] initialRows, uint rowToDelete, uint[] expectedRows)
    {
        foreach (var initialRow in initialRows)
        {
            await InsertAtEndOfTestFile(new StorageRecord
            {
                Timestamp = initialRow,
                LhsId = initialRow,
                RhsId = initialRow,
                RelationshipId = initialRow
            });
        }

        using (var storage = new Storage { FillFactor = 10 })
        {
            await storage.DeleteRowAsync(new StorageRecord
            {
                Timestamp = rowToDelete,
                LhsId = rowToDelete,
                RhsId = rowToDelete,
                RelationshipId = rowToDelete
            });
        }

        var actual = await File.ReadAllBytesAsync(Storage.BackingFilePath());

        var expected = IsRow(expectedRows[0], expectedRows[0], expectedRows[0], expectedRows[0]);
        for (var i = 1; i < expectedRows.Length; i++)
        {
            expected = expected.Concat(
                IsRow(expectedRows[i], expectedRows[i], expectedRows[i], expectedRows[i])
            );
        }

        actual.Should().BeEquivalentTo(expected);
    }
}
