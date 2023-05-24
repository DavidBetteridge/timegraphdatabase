namespace TimeGraphDatabase.Engine;

public readonly record struct StorageRecord(ulong Timestamp, uint LhsId, uint RhsId, uint RelationshipId)
{
    public byte[] ToByteArray()
    {
        return BitConverter.GetBytes(Timestamp)
            .Concat(BitConverter.GetBytes(LhsId))
            .Concat(BitConverter.GetBytes(RhsId))
            .Concat(BitConverter.GetBytes(RelationshipId)).ToArray();
    }
}
