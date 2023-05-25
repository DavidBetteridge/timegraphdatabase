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
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
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
            await InsertAtEndOfFile(new StorageRecord
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

    private static IEnumerable<byte> IsRow(ulong i, uint i1, uint i2, uint i3)
    {
        return BitConverter.GetBytes(i)
            .Concat(BitConverter.GetBytes(i1))
            .Concat(BitConverter.GetBytes(i2))
            .Concat(BitConverter.GetBytes(i3));
    }

    private static IEnumerable<byte> IsFiller() => IsRow(0, 0, 0, 0);

    [Fact]
    public async Task RowsCanBeInsertedAtAFillerLocation()
    {
        // Given a file containing three rows:  1, FILLER, 3
        await InsertAtEndOfFile(new StorageRecord
        {
            Timestamp = 1,
            LhsId = 1,
            RhsId = 1,
            RelationshipId = 1
        });

        await InsertAtEndOfFile(new StorageRecord
        {
            Timestamp = 0,
            LhsId = 0,
            RhsId = 0,
            RelationshipId = 0
        });

        await InsertAtEndOfFile(new StorageRecord
        {
            Timestamp = 3,
            LhsId = 3,
            RhsId = 3,
            RelationshipId = 3
        });

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
            IsRow(1, 1, 1, 1)
                .Concat(IsRow(2, 2, 2, 2))
                .Concat(IsRow(3, 3, 3, 3));
        actual.Should().BeEquivalentTo(expected);
    }

    private static async Task InsertAtEndOfFile(StorageRecord record)
    {
        await using var file = File.Open(Storage.BackingFilePath(), FileMode.OpenOrCreate);
        file.Seek(0, SeekOrigin.End);
        await file.WriteAsync(BitConverter.GetBytes(record.Timestamp).AsMemory(0, 8));
        await file.WriteAsync(BitConverter.GetBytes(record.LhsId).AsMemory(0, 4));
        await file.WriteAsync(BitConverter.GetBytes(record.RhsId).AsMemory(0, 4));
        await file.WriteAsync(BitConverter.GetBytes(record.RelationshipId).AsMemory(0, 4));
    }
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
 * When rows are deleted they are replaced by a filler  ✅
 *
 *
 * QUERY TESTS
 * All entries in a single file < X
 * All entries from split files < X
 * Entries with predicate ( lhs, rhs, relation => bool )
 */