using Persisted;
using System;
using System.Collections.Generic;

namespace Persisted.Typed
{
    /// <summary>
    /// A schema component for 8-bit integers
    /// </summary>
    internal class ByteSchema : Schema<byte>
    {
        internal override int GetSize(Encoding encoding)
        {
            return encoding.EncodingSizeForByte;
        }

        internal override byte Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            return encoding.ReadByte(image.PrimaryContainer, ref position);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, byte element)
        {
            encoding.WriteByte(image.PrimaryContainer, ref position, element);
        }
    }

    /// <summary>
    /// A schema component for classical, 32-bit integers
    /// </summary>
    internal class IntSchema : Schema<int>
    {
        internal override int GetSize(Encoding encoding)
        {
            return encoding.EncodingSizeForInt;
        }

        internal override int Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            return encoding.ReadInt(image.PrimaryContainer, ref position);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, int element)
        {
            encoding.WriteInt(image.PrimaryContainer, ref position, element);
        }
    }

    /// <summary>
    /// A schema component for long, 64-bit integers
    /// </summary>
    internal class LongSchema : Schema<long>
    {
        internal override int GetSize(Encoding encoding)
        {
            return encoding.EncodingSizeForLong;
        }

        internal override Int64 Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            return encoding.ReadLong(image.PrimaryContainer, ref position);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, long element)
        {
            encoding.WriteLong(image.PrimaryContainer, ref position, element);
        }
    }
}