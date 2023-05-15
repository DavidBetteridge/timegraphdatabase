namespace TimeGraphDatabase.Engine;

public readonly record struct StorageRecord(long Timestamp, long LhsId, long RhsId, long RelationshipId);
