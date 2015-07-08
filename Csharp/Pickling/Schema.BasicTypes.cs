using System;
using Common;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    /// <summary>
    /// A schema component for 8-bit integers
    /// </summary>
    internal class ByteSchema : Schema<byte>
    {
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForByte;
        }

        internal override int GetDynamicSize(byte element)
        {
            return 0;
        }

        internal override byte Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            CheckReadPreconditions(fixedStorage, dynamicStorage);
            return Encoding.ReadByte(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, byte element)
        {
            CheckWritePreconditions(fixedStorage, dynamicStorage, element);
            Encoding.WriteByte(fixedStorage, element);
        }
    }

    /// <summary>
    /// A schema component for classical, 32-bit integers
    /// </summary>
    internal class IntSchema : Schema<int>
    {
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForInt;
        }

        internal override int GetDynamicSize(int element)
        {
            return 0;
        }

        internal override int Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            CheckReadPreconditions(fixedStorage, dynamicStorage);
            return Encoding.ReadInt(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, int element)
        {
            CheckWritePreconditions(fixedStorage, dynamicStorage, element);
            Encoding.WriteInt(fixedStorage, element);
        }
    }

    /// <summary>
    /// A schema component for long, 64-bit integers
    /// </summary>
    internal class LongSchema : Schema<long>
    {
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForLong;
        }

        internal override int GetDynamicSize(long element)
        {
            return 0;
        }

        internal override long Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            CheckReadPreconditions(fixedStorage, dynamicStorage);
            return Encoding.ReadLong(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, long element)
        {
            CheckWritePreconditions(fixedStorage, dynamicStorage, element);
            Encoding.WriteLong(fixedStorage, element);
        }
    }


    /// <summary>
    /// A schema component for strings
    /// </summary>
    internal class StringSchema : Schema<string>
    {
        // TODO: schemas are stateful, therefore construct explicitly
        private char[] buffer_ = new char[128];

        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForOffset;
        }

        internal override int GetDynamicSize(string element)
        {
            return Encoding.EncodingSizeForString(element.Length);
        }

        internal override string Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            CheckReadPreconditions(fixedStorage, dynamicStorage);
            int length = Encoding.ReadInt(fixedStorage);
            return Encoding.ReadString(dynamicStorage, length, ref buffer_);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, string element)
        {
            CheckWritePreconditions(fixedStorage, dynamicStorage, element);
            Encoding.WriteInt(fixedStorage, element.Length);
            Encoding.WriteString(dynamicStorage, element);
        }
    }
}
