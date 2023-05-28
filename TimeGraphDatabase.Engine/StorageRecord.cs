using System.Buffers.Binary;

namespace TimeGraphDatabase.Engine;

public readonly record struct StorageRecord(ulong Timestamp, uint LhsId, uint RhsId, uint RelationshipId)
{
    public byte[] ToByteArray()
    {
        if (BitConverter.IsLittleEndian)
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(Timestamp))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(LhsId)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(RhsId)))
                .Concat(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(RelationshipId))).ToArray();
        else
            return BitConverter.GetBytes(Timestamp)
                .Concat(BitConverter.GetBytes(LhsId))
                .Concat(BitConverter.GetBytes(RhsId))
                .Concat(BitConverter.GetBytes(RelationshipId)).ToArray();
    }
}
