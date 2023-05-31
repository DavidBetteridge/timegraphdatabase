﻿using System.Buffers.Binary;
using System.Diagnostics;
using Spectre.Console;
using TimeGraphDatabase.Engine;

File.Delete("nodes.index");
File.Delete("nodes.content");

// var sw = new Stopwatch();
// sw.Start();
//
// var numberOfRows = 5_000_000;
// using var nodeStorage = new IdValueStorage();
// for (int i = 0; i < numberOfRows; i++)
// {
//     await nodeStorage.InsertAsync(i.ToString());
// }
// Console.WriteLine($"{sw.Elapsed.TotalSeconds}s to insert {numberOfRows} rows.");
//
// sw.Restart();
//
// for (int i = 0; i < numberOfRows; i++)
// {
//    var content = await nodeStorage.GetByIdAsync(i+1);
//    if (content != i.ToString())
//        throw new Exception();
// }
//
// sw.Stop();
//
// Console.WriteLine($"{sw.Elapsed.TotalSeconds}s to read {numberOfRows} rows.");
//



var sw = new Stopwatch();
sw.Start();

var numberOfRows = 5_000_000;
using var nodeStorage = new IdValueStorage();
for (int i = 0; i < numberOfRows; i++)
{
    var nodeId = await nodeStorage.InsertAsync(i.ToString());
    var content = await nodeStorage.GetByIdAsync(nodeId);
    if (content != i.ToString())
        throw new Exception();
}

sw.Stop();

Console.WriteLine($"{sw.Elapsed.TotalSeconds}s to insert/read {numberOfRows} rows.");
return;




const int NumberOfRows = 2600;
//const int NumberOfRows = 250000;
File.Delete(Storage.BackingFilePath());
using (var storage = new Storage { FillFactor = 10 })
{
    //for (uint i = 1; i <= NumberOfRows; i++)
    for (uint i = NumberOfRows; i > 0; i--)
    {
        await storage.InsertRowAsync(new StorageRecord
        {
            Timestamp = (ulong) new DateTimeOffset(2023,1,1,0,0,0, TimeSpan.Zero).AddDays(i).ToUnixTimeMilliseconds(),
            LhsId = i,
            RhsId = i,
            RelationshipId = i
        });
        
        if (!storage.IsValid())
            throw new Exception($"Failed at {i}");
    }
    // if (!storage.IsValid())
    //     throw new Exception($"Failed before defrag");
    // await storage.DefragAsync();
    // if (!storage.IsValid())
    //     throw new Exception($"Failed after defrag");
    
    // await storage.InsertRowAsync(new StorageRecord
    // {
    //     Timestamp = (ulong) new DateTimeOffset(2023,1,1,0,0,0, TimeSpan.Zero).AddDays(49).ToUnixTimeMilliseconds(),
    //     LhsId = 49,
    //     RhsId = 49,
    //     RelationshipId = 49
    // });
    // if (!storage.IsValid())
    //     throw new Exception($"Failed at manual 49");

}

sw.Stop();

Console.WriteLine($"{sw.Elapsed.TotalSeconds}s to insert {NumberOfRows} rows.");
return;
//
// using (var storage = new Storage { FillFactor = 10 })
// { 
//     await storage.DefragAsync();
// }
var fileContents = await File.ReadAllBytesAsync(Storage.BackingFilePath());
    //var fileContents = await File.ReadAllBytesAsync("/Users/davidbetteridge/Personal/TimeGraphDatabase/TimeGraphDatabase.Tests/bin/Debug/net7.0/database.graph");
//var numberOfRows = fileContents.Length / Storage.BytesPerRow;
Console.WriteLine($"{numberOfRows} rows read");
var table = new Table();
table.AddColumn("Row Number");
table.AddColumn("Timestamp");
table.AddColumn("Source");
table.AddColumn("Destination");
table.AddColumn("Relationship");


for (var rowNumber = 1; rowNumber <= numberOfRows; rowNumber++)
{
    var row = fileContents[((rowNumber - 1) * Storage.BytesPerRow)..(rowNumber * Storage.BytesPerRow)];

    var timestamp = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt64(row.AsSpan()[..8]));
    var when = DateTimeOffset.FromUnixTimeMilliseconds((long)timestamp);
    
    var lhs = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(row.AsSpan()[8..12]));
    var rhs = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(row.AsSpan()[12..16]));
    var relation = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(row.AsSpan()[16..20]));
    
    if (timestamp == 0)
        table.AddRow($"[red]{rowNumber:0000}[/]","[teal]FILLER[/]","[teal]FILLER[/]","[teal]FILLER[/]","[teal]FILLER[/]");
    else
    {
        table.AddRow($"[red]{rowNumber:0000}[/]",$"[blue]{when:O}[/]",$"[green]0x{lhs:x8} ({lhs})[/]",$"[maroon]0x{rhs:x8}[/]",$"[purple]0x{relation:x8}[/]");
    }
}
AnsiConsole.Write(table);