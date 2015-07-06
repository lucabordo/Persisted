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
        
        public static byte ReadByte(ByteSegmentReadView source)
        {
            long result = ReadIntegralType(source, CharCountForByte);
            return (byte)result;
        }
        
        public static int ReadInt(ByteSegmentReadView source)
        {
            long result = ReadIntegralType(source, CharCountForInt);
            return (int)result;
        }
        
        public static long ReadLong(ByteSegmentReadView source)
        {
            long result = ReadIntegralType(source, CharCountForLong);
            return result;
        }
        
        private static long ReadIntegralType(ByteSegmentReadView source, int digits)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            // Note that we are assuming the following min and max values 
            //  9223372036854775807;
            // -9223372036854775808;
            // We have more negative values than positive ones. 
            // To read -9223372036854775808 correctly we need to read everything negated

            // Possible '-' sign at start
            int i = 1;
            var first = ReadChar(source);

            bool negative = (first == '-');
            long minusResults = negative ? 0 : -ToInt(first);

            // digits 
            for (; i < digits; ++i)
            {
                char next = ReadChar(source);

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
                if (ReadChar(source) != ' ')
                {
                    throw new Exception();
                }
            }

            return negative ? minusResults : (-minusResults);
        }
        
        public static void WriteByte(ByteSegmentWriteView target, byte value)
        {
            WriteIntegralType(target, value, CharCountForByte);
        }
        
        public static void WriteInt(ByteSegmentWriteView target, int value)
        {
            WriteIntegralType(target, value, CharCountForInt);
        }
        
        public static void WriteLong(ByteSegmentWriteView target, long value)
        {
            WriteIntegralType(target, value, CharCountForLong);
        }

        private static void WriteIntegralType(ByteSegmentWriteView target, long value, int digits)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            bool negative = value < 0;
            int byteId = digits * 2;

            // Write the digits backwards, making sure to insert one digit if the value is 0

            var converter = new Converter16('0');
            if (value == 0)
            {
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            for (long v = negative ? value : -value; v != 0; v /= 10)
            {
                converter = new Converter16(ToChar(-(v % 10)));
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            // Precede this text by a sign if needed and (backwards) by spaces

            if (negative)
            {
                converter = new Converter16('-');
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            converter = new Converter16(' ');
            while (byteId != 0)
            {
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            target.MoveForward(2 * digits);
        }

        #endregion

        #region Read and write references

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadReference(ByteSegmentReadView source)
        {
            SkipReferenceHeader(source);
            return ReadLong(source);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteReference(ByteSegmentWriteView target, long value)
        {
            WriteReferenceHeader(target);
            WriteLong(target, value);
        }

        #endregion

        #region Read and Write characters

        public static string ReadString(ByteSegmentReadView source, int length, ref char[] charBuffer)
        {
            while (charBuffer.Length < length)
                charBuffer = new char[charBuffer.Length * 2];
            for (int i = 0; i < length; ++i)
                charBuffer[i] = ReadChar(source);
            return new string(charBuffer, 0, length);
        }

        public static void WriteString(ByteSegmentWriteView writer, string value)
        {
            foreach (char c in value)
                WriteChar(writer, c);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipExpectedCharacter(ByteSegmentReadView source, char expectedChar)
        {
            var nextChar = ReadChar(source);
            if (nextChar != expectedChar)
                throw new Exception();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static char ReadChar(ByteSegmentReadView source)
        {
            var byte1 = source.NextChar;
            var byte2 = source.NextChar;
            var converter = new Converter16(byte1, byte2);
            return converter.AsChar;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteChar(ByteSegmentWriteView writer, char c)
        {
            var converter = new Converter16(c);
            writer.NextChar = converter.Byte0;
            writer.NextChar = converter.Byte1;
        }

        /// <summary>
        /// Insert a separator between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WritePropertySeparator(ByteSegmentWriteView writer)
        {
            WriteChar(writer, ElementSeparator);
        }

        /// <summary>
        /// Read and skip an expected separator used between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipPropertySeparator(ByteSegmentReadView source)
        {
            SkipExpectedCharacter(source, ElementSeparator);
        }

        /// <summary>
        /// Insert a separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteObjectSeparator(ByteSegmentWriteView writer)
        {
            writer.NextChar = (byte)'\r';
            writer.NextChar = (byte)'\n';
        }

        /// <summary>
        /// Read and skip an expected separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipObjectSeparator(ByteSegmentReadView source)
        {
            char c1 = (char)source.NextChar;
            char c2 = (char)source.NextChar;

            if (c1 != '\r' || c2 != '\n')
                throw new Exception();
        }

        /// <summary>
        /// Insert a header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteReferenceHeader(ByteSegmentWriteView writer)
        {
            WriteChar(writer, ReferenceHeader);
        }

        /// <summary>
        /// Read and skip an expected header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipReferenceHeader(ByteSegmentReadView source)
        {
            SkipExpectedCharacter(source, ReferenceHeader);
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
