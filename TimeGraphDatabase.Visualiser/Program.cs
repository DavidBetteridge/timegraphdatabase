﻿using System.Buffers.Binary;
using System.Diagnostics;
using Spectre.Console;
using TimeGraphDatabase.Engine;



var sw = new Stopwatch();
sw.Start();

const int NumberOfRows = 1250;
// const int NumberOfRows = 250000;
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
    }
}

sw.Stop();

Console.WriteLine($"{sw.Elapsed.TotalSeconds}s to insert {NumberOfRows} rows.");

//
// using (var storage = new Storage { FillFactor = 10 })
// { 
//     await storage.DefragAsync();
// }
var fileContents = await File.ReadAllBytesAsync(Storage.BackingFilePath());
    //var fileContents = await File.ReadAllBytesAsync("/Users/davidbetteridge/Personal/TimeGraphDatabase/TimeGraphDatabase.Tests/bin/Debug/net7.0/database.graph");
var numberOfRows = fileContents.Length / Storage.BytesPerRow;
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
        table.AddRow($"[red]{rowNumber:0000}[/]",$"[blue]{when:O}[/]",$"[green]0x{lhs:x8}[/]",$"[maroon]0x{rhs:x8}[/]",$"[purple]0x{relation:x8}[/]");
    }
}
AnsiConsole.Write(table);