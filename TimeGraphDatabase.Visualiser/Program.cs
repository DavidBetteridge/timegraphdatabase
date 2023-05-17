using Spectre.Console;
using TimeGraphDatabase.Engine;

File.Delete(Storage.BackingFilePath());
using (var storage = new Storage { FillFactor = 10 })
{
    for (uint i = 0; i < 100; i++)
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

var fileContents = await File.ReadAllBytesAsync(Storage.BackingFilePath());
var numberOfRows = fileContents.Length / Storage.BytesPerRow;

var table = new Table();
table.AddColumn("Row Number");
table.AddColumn("Timestamp");
table.AddColumn("Source");
table.AddColumn("Destination");
table.AddColumn("Relationship");


for (var rowNumber = 1; rowNumber <= numberOfRows; rowNumber++)
{
    var row = fileContents[((rowNumber - 1) * Storage.BytesPerRow)..(rowNumber * Storage.BytesPerRow)];

    var timestamp = BitConverter.ToUInt64(row.AsSpan()[..8]);
    var when = DateTimeOffset.FromUnixTimeMilliseconds((long)timestamp);
    
    var lhs = BitConverter.ToUInt32(row.AsSpan()[8..12]);
    var rhs = BitConverter.ToUInt32(row.AsSpan()[12..16]);
    var relation = BitConverter.ToUInt32(row.AsSpan()[16..20]);
    
    if (timestamp == 0)
        table.AddRow($"[red]{rowNumber:0000}[/]","[teal]FILLER[/]","[teal]FILLER[/]","[teal]FILLER[/]","[teal]FILLER[/]");
    else
    {
        table.AddRow($"[red]{rowNumber:0000}[/]",$"[blue]{when:O}[/]",$"[green]0x{lhs:x8}[/]",$"[maroon]0x{rhs:x8}[/]",$"[purple]0x{relation:x8}[/]");
    }
}
AnsiConsole.Write(table);