namespace TimeGraphDatabase.Engine;

public readonly record struct StorageRecord(ulong Timestamp, ulong LhsId, ulong RhsId, ulong RelationshipId);
