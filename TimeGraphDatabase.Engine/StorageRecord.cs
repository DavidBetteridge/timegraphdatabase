namespace TimeGraphDatabase.Engine;

public readonly record struct StorageRecord(ulong Timestamp, uint LhsId, uint RhsId, uint RelationshipId);
