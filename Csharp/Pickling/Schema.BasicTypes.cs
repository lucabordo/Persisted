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
        internal override bool IsFixedSize
        {
            get { return true; }
        }

        internal override int GetDynamicSize(byte element)
        {
            return 0;
        }

        internal override byte Read(ByteSegmentReadView segment)
        {
            return Encoding.ReadByte(segment);
        }

        internal override void Write(ByteSegmentWriteView segment, byte element)
        {
            Encoding.WriteByte(segment, element);
        }
    }

    /// <summary>
    /// A schema component for classical, 32-bit integers
    /// </summary>
    internal class IntSchema : Schema<int>
    {
        internal override bool IsFixedSize
        {
            get { return true; }
        }

        internal override int GetDynamicSize(int element)
        {
            return 0;
        }

        internal override int Read(ByteSegmentReadView segment)
        {
            return Encoding.ReadInt(segment);
        }

        internal override void Write(ByteSegmentWriteView segment, int element)
        {
            Encoding.WriteInt(segment, element);
        }
    }

    /// <summary>
    /// A schema component for long, 64-bit integers
    /// </summary>
    internal class LongSchema : Schema<long>
    {
        internal override bool IsFixedSize
        {
            get { return true; }
        }

        internal override int GetDynamicSize(long element)
        {
            return 0;
        }

        internal override long Read(ByteSegmentReadView segment)
        {
            return Encoding.ReadLong(segment);
        }

        internal override void Write(ByteSegmentWriteView segment, long element)
        {
            Encoding.WriteLong(segment, element);
        }
    }


    /// <summary>
    /// A schema component for strings
    /// </summary>
    internal class StringSchema : Schema<string>
    {
        // TODO: schemas are stateful, therefore construct explicitly
        private char[] buffer_ = new char[128];

        internal override bool IsFixedSize
        {
            get { return false; }
        }

        internal override int GetDynamicSize(string element)
        {
            return Encoding.EncodingSizeForString(element.Length);
        }

        internal override string Read(ByteSegmentReadView segment)
        {

            int length = Encoding.ReadInt(segment);
            return Encoding.ReadString(segment, length, ref buffer_);
        }

        internal override void Write(ByteSegmentWriteView segment, string element)
        {
            Encoding.WriteInt(segment, element.Length);
            Encoding.WriteString(segment, element);
        }
    }
}
