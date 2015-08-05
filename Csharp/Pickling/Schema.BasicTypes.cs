using System;
using Common;

#if USE_REFLECTION_EMIT
    using System.Reflection.Emit;
#endif

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
            return Encoding.EncodingSizeForByte;
        }

        internal override byte Read(ByteBufferReadCursor segment)
        {
            return Encoding.ReadByte(segment);
        }

        internal override void Write(ByteBufferWriteCursor segment, byte element)
        {
            Encoding.WriteByte(segment, element);
        }

        internal override void CompileReadMethod(ILGenerator generator)
        {
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Call, typeof(Encoding).GetMethod("ReadByte", new Type[] { typeof(ByteBufferReadCursor) }));
            generator.Emit(OpCodes.Ret);
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
            return Encoding.EncodingSizeForInt;
        }

        internal override int Read(ByteBufferReadCursor segment)
        {
            return Encoding.ReadInt(segment);
        }

        internal override void Write(ByteBufferWriteCursor segment, int element)
        {
            Encoding.WriteInt(segment, element);
        }

        internal override void CompileReadMethod(ILGenerator generator)
        {
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Call, typeof(Encoding).GetMethod("ReadInt", new Type[] { typeof(ByteBufferReadCursor) }));
            generator.Emit(OpCodes.Ret);
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
            return Encoding.EncodingSizeForLong;
        }

        internal override long Read(ByteBufferReadCursor segment)
        {
            return Encoding.ReadLong(segment);
        }

        internal override void Write(ByteBufferWriteCursor segment, long element)
        {
            Encoding.WriteLong(segment, element);
        }

        internal override void CompileReadMethod(ILGenerator generator)
        {
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Call, typeof(Encoding).GetMethod("ReadLong", new Type[] { typeof(ByteBufferReadCursor) }));
            generator.Emit(OpCodes.Ret);
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
            return 
                Encoding.EncodingSizeForInt + 
                Encoding.EncodingSizeForStringStartIndicator +
                Encoding.EncodingSizeForString(element.Length) + 
                Encoding.EncodingSizeForStringEndIndicator;
        }

        internal override string Read(ByteBufferReadCursor segment)
        {
            int length = Encoding.ReadInt(segment);
            Encoding.SkipStringStartIndicator(segment);
            var result = Encoding.ReadString(segment, length, ref buffer_);
            Encoding.SkipStringEndIndicator(segment);
            return result;
        }

        internal override void Write(ByteBufferWriteCursor segment, string element)
        {
            Encoding.WriteInt(segment, element.Length);
            Encoding.WriteStringStartIndicator(segment);
            Encoding.WriteString(segment, element);
            Encoding.WriteStringEndIndicator(segment);
        }

        internal override void CompileReadMethod(ILGenerator generator)
        {
            generator.Emit(OpCodes.Ldarg_1);
            generator.Emit(OpCodes.Call, typeof(Encoding).GetMethod("ReadString", new Type[] { typeof(ByteBufferReadCursor) }));
        }
    }
}
