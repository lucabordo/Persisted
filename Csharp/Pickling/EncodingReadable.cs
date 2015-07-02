using Common;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Pickling
{
    /// <summary>
    /// Low-level methods that define how numerical and basic types and encoded into bytes.
    /// </summary>
    /// <remarks>
    /// So far we have a single class - but the design should make it easy to move to interface 
    /// and various encodings if we need to decide encoding dynamically.
    /// Not that encoding objects are NOT THREAD-SAFE so one instance per thread should be created
    /// </remarks>
    internal static class ReadableEncoding
    {
        #region Constants and fields

        internal const char ElementSeparator = ',';
        internal const string EndOfTuple = "\r\n";
        internal const char ReferenceHeader = '@';

        // Number of characters in the representation of basic value types
        private static readonly int CharCountForByte = byte.MaxValue.ToString().Length;
        private static readonly int CharCountForInt = int.MinValue.ToString().Length;
        private static readonly int CharCountForLong = long.MinValue.ToString().Length;

        // number of bytes in the encoding of basic value types
        public static readonly int EncodingSizeForByte = 2 * byte.MaxValue.ToString().Length;
        public static readonly int EncodingSizeForInt = 2 * int.MinValue.ToString().Length;
        public static readonly int EncodingSizeForLong = 2 * long.MinValue.ToString().Length;

        public static readonly int EncodingSizeForReservedChar = 2;
        public static readonly int EncodingSizeForSeparator = 2;
        public static readonly int EncodingSizeForEndOfTuple = 4;

        #endregion

        #region Read and Write numbers

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte ReadByte(ByteSegmentReadView source, ref int position)
        {
            long result = ReadIntegralType(source, CharCountForByte, ref position);
            return (byte)result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadInt(ByteSegmentReadView source, ref int position)
        {
            long result = ReadIntegralType(source, CharCountForInt, ref position);
            return (int)result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadLong(ByteSegmentReadView source, ref int position)
        {
            long result = ReadIntegralType(source, CharCountForLong, ref position);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long ReadIntegralType(ByteSegmentReadView source, int digits, ref int position)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            // Note that we are assuming the following min and max values 
            //  9223372036854775807;
            // -9223372036854775808;
            // We have more negative values than positive ones. 
            // To read -9223372036854775808 correctly we need to read everything negated

            long minusResults = 0;
            bool negative = false;
            int i = 1;

            // Possible '-' sign at start
            var first = ReadChar(source, ref position);

            if (first == '-')
            {
                negative = true;
            }
            else
            {
                minusResults = -ToInt(first);
            }

            // digits 
            for (; i < digits; ++i)
            {
                char next = ReadChar(source, ref position);

                if (next == ' ')
                {
                    break;
                }
                else
                {
                    minusResults = (10 * minusResults) - ToInt(next);
                }
            }

            // remaining spaces
            for (++i; i < digits; ++i)
            {
                if (ReadChar(source, ref position) != ' ')
                {
                    throw new Exception();
                }
            }

            return negative ? minusResults : (-minusResults);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteByte(ByteSegmentWriteView target, ref int position, byte value)
        {
            WriteIntegralType(target, ref position, value, CharCountForByte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt(ByteSegmentWriteView target, ref int position, int value)
        {
            WriteIntegralType(target, ref position, value, CharCountForInt);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteLong(ByteSegmentWriteView target, ref int position, long value)
        {
            WriteIntegralType(target, ref position, value, CharCountForLong);
        }

        private static void WriteIntegralType(ByteSegmentWriteView target, ref int position, long value, int digits)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            bool negative = value < 0;
            int byteId = digits * 2;

            // Write the digits backwards, making sure to insert one digit if the value is 0

            if (value == 0)
            {
                var converter = new Converter16('0');
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            for (long v = negative ? value : -value; v != 0; v /= 10)
            {
                var converter = new Converter16(ToChar(-(v % 10)));
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            // Precede this text by a sign if needed and (backwards) by spaces

            if (negative)
            {
                var converter = new Converter16('-');
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            while (byteId != 0)
            {
                var converter = new Converter16(' ');
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            position += 2 * digits;
        }

        #endregion

        #region Read and write references

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadReference(ByteSegmentReadView source, ref int position)
        {
            SkipReferenceHeader(source, ref position);
            return ReadLong(source, ref position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteReference(ByteSegmentWriteView target, ref int position, long value)
        {
            WriteReferenceHeader(target, ref position);
            WriteLong(target, ref position, value);
        }

        #endregion

        #region Read and Write characters

        public static string ReadString(ByteSegmentReadView source, ref int position, int length, ref char[] charBuffer)
        {
            while (charBuffer.Length < length)
                charBuffer = new char[charBuffer.Length * 2];
            for (int i = 0; i < length; ++i)
                charBuffer[i] = ReadChar(source, ref position);
            return new string(charBuffer, 0, length);
        }

        public static void WriteString(ByteSegmentWriteView writer, string value, ref int position)
        {
            foreach (char c in value)
                WriteChar(writer, c, ref position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipExpectedCharacter(ByteSegmentReadView source, char expectedChar, ref int position)
        {
            var nextChar = ReadChar(source, ref position);
            if (nextChar != expectedChar)
                throw new Exception();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static char ReadChar(ByteSegmentReadView source, ref int position)
        {
            var byte1 = source[position++];
            var byte2 = source[position++];
            var converter = new Converter16(byte1, byte2);
            return converter.AsChar;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteChar(ByteSegmentWriteView writer, char c, ref int position)
        {
            var converter = new Converter16(c);
            writer[position++] = converter.Byte0;
            writer[position++] = converter.Byte1;
        }

        /// <summary>
        /// Insert a separator between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WritePropertySeparator(ByteSegmentWriteView writer, ref int position)
        {
            WriteChar(writer, ElementSeparator, ref position);
        }

        /// <summary>
        /// Read and skip an expected separator used between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipPropertySeparator(ByteSegmentReadView source, ref int position)
        {
            SkipExpectedCharacter(source, ElementSeparator, ref position);
        }

        /// <summary>
        /// Insert a separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteObjectSeparator(ByteSegmentWriteView writer, ref int position)
        {
            writer[position++] = (byte)'\r';
            writer[position++] = (byte)'\n';
        }

        /// <summary>
        /// Read and skip an expected separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipObjectSeparator(ByteSegmentReadView source, ref int position)
        {
            char c1 = (char)source[position++];
            char c2 = (char)source[position++];

            if (c1 != '\r' || c2 != '\n')
                throw new Exception();
        }

        /// <summary>
        /// Insert a header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteReferenceHeader(ByteSegmentWriteView writer, ref int position)
        {
            WriteChar(writer, ReferenceHeader, ref position);
        }

        /// <summary>
        /// Read and skip an expected header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipReferenceHeader(ByteSegmentReadView source, ref int position)
        {
            SkipExpectedCharacter(source, ReferenceHeader, ref position);
        }

        #endregion

        #region Private

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ToInt(char b)
        {
            if ('0' <= b && b <= '9')
            {
                return b - '0';
            }
            else
            {
                throw new Exception();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static char ToChar(long b)
        {
            if (0 <= b && b <= 9)
            {
                return (char)(b + '0');
            }
            else
            {
                throw new Exception();
            }
        }

        #endregion
    }
}
