var relationships = new List<Relationship>();
var nodes = new Dictionary<long, Node>();
var relations = new Dictionary<long, Relation>();
var idProvider = new IdProvider();

var mary = new Node { Label = "Mary", _id = idProvider.Next()};
nodes.Add(mary._id, mary);

var edward = new Node { Label = "Edward", _id = idProvider.Next()};
nodes.Add(edward._id, edward);

var shipped = new Relation { Label = "Shipped", _id = idProvider.Next() };
relations.Add(shipped._id, shipped);
   
var filrting = new Relation { Label = "Shipped", _id = idProvider.Next() };
relations.Add(shipped._id, shipped);


var groupMember = new Relationship
{
    Timestamp = DateTimeOffset.UtcNow.AddDays(1),
    LHS = mary,
    RHS = edward,
    Relation = shipped
};
relationships.Add(groupMember);


using var nodesFile = File.CreateText("nodes.txt");
foreach (var node in nodes)
    nodesFile.WriteLine($"{node.Key},{node.Value.Label}");

using var relationsFile = File.CreateText("relations.txt");
foreach (var relation in relations)
    nodesFile.WriteLine($"{relation.Key},{relation.Value.Label}");

const decimal FillFactor = .1M;
const long Filler = 0;

// Create file from scratch
using var relationshipsFile = File.Create("relationships.graph");
using var writer = new BinaryWriter(relationshipsFile);
short numberOfRowsSinceFiller = 0;

writer.Write(numberOfRowsSinceFiller);
foreach (var relationship in relationships)     
{
    writer.Write(relationship.Timestamp.ToUnixTimeMilliseconds());
    writer.Write(relationship.LHS._id);
    writer.Write(relationship.RHS._id);
    writer.Write(relationship.Relation._id);
    numberOfRowsSinceFiller++;

    // Every X rows we insert a filler row
    if (numberOfRowsSinceFiller == FillFactor)
    {
        writer.Write(relationship.Timestamp.ToUnixTimeMilliseconds());
        writer.Write(Filler);
        writer.Write(Filler);
        writer.Write(Filler);
        numberOfRowsSinceFiller = 0;
    }
}





//nodes
// id name

// relationships
// id label

// relations
// timestamp lhs_id rhs_id relationship_id

// meta
// timestamp object_id properties


record Relationship
{
    public DateTimeOffset Timestamp { get; set; }

    public Node LHS { get; set; }

    public Node RHS { get; set; }

    public Relation Relation { get; set; }
}

record Node
{
    public long _id { get; set; }
    public string Label { get; set; }
}

record Relation
{
    public long _id { get; set; }
    public string Label { get; set; }
}

class IdProvider
{
    private long _nextId = 0;

    public long Next()
    {
        return ++_nextId;
    }
}

