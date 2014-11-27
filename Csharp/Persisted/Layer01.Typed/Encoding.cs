using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Persisted.Typed
{
    using Persisted.Utils;
    using ByteTable = Persisted.Utils.TableFromContainer<byte>;

    /// <summary>
    /// Low-level methods that define how numerical and basic types and encoded into bytes.
    /// </summary>
    /// <remarks>
    /// So far we have a single class - but the design should make it easy to move to interface 
    /// and various encodings if we need to decide encoding dynamically.
    /// Not that encoding objects are NOT THREAD-SAFE so one instance per thread should be created
    /// </remarks>
    internal class Encoding
    {
        #region Constants and fields

        internal const char ElementSeparator = ',';
        internal const string EndOfTuple = "\r\n";
        internal const char ReferenceHeader = '@';

        // Number of characters in the representation of basic value types
        private readonly int CharCountForByte = byte.MaxValue.ToString().Length;
        private readonly int CharCountForInt = int.MinValue.ToString().Length;
        private readonly int CharCountForLong = long.MinValue.ToString().Length;

        // number of bytes in the encoding of basic value types
        public readonly int EncodingSizeForByte = 2 * byte.MaxValue.ToString().Length;
        public readonly int EncodingSizeForInt = 2 * int.MinValue.ToString().Length;
        public readonly int EncodingSizeForLong = 2 * long.MinValue.ToString().Length;

        public readonly int EncodingSizeForReservedChar = 2;
        public readonly int EncodingSizeForSeparator = 2;
        public readonly int EncodingSizeForEndOfTuple = 4;

        /// <summary>
        /// A buffer reused for the writes
        /// </summary>
        /// <remarks>
        /// NOTE THAT THIS MAKES INSTANCES OF THIS CLASS STATEFULL AND NOT THREAD SAFE
        /// </remarks>
        private char[] _charBuffer = new char[64];

        #endregion

        #region Read and Write numbers

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte(ByteTable source, ref long position)
        {
            long result = ReadIntegralType(source, CharCountForByte, ref position);
            return (byte)result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt(ByteTable source, ref long position)
        {
            long result = ReadIntegralType(source, CharCountForInt, ref position);
            return (int)result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadLong(ByteTable source, ref long position)
        {
            long result = ReadIntegralType(source, CharCountForLong, ref position);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ReadIntegralType(ByteTable source, int digits, ref long position)
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
        public void WriteByte(ByteTable source, ref long position, byte value)
        {
            WriteIntegralType(source, ref position, value, CharCountForByte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt(ByteTable source, ref long position, int value)
        {
            WriteIntegralType(source, ref position, value, CharCountForInt);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteLong(ByteTable source, ref long position, long value)
        {
            WriteIntegralType(source, ref position, value, CharCountForLong);
        }

        private void WriteIntegralType(ByteTable target, ref long position, long value, int digits)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            // Note that we are assuming the following min and max values 
            //  9223372036854775807;
            // -9223372036854775808;
            // We have more negative values than positive ones. 
            // To read -9223372036854775808 correctly we need to write everything negated
            int writeBufferCount = 0;
            bool negative = value < 0;
            long v = (negative) ? value : -value;

            while (v != 0)
            {
                _charBuffer[writeBufferCount++] = ToChar(-(v % 10));
                v /= 10;
            }

            if (writeBufferCount == 0)
            {
                Debug.Assert(value == 0);
                _charBuffer[writeBufferCount++] = ToChar(0);
            }

            if (negative)
            {
                WriteChar(target, '-', ref position);
            }

            for (int i = writeBufferCount - 1; i >= 0; --i)
            {
                WriteChar(target, _charBuffer[i], ref position);
            }

            for (int i = writeBufferCount + (negative ? 1 : 0); i < digits; ++i)
            {
                WriteChar(target, ' ', ref position);
            }
        }

        #endregion

        #region Read and write references

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ReadReference(ByteTable source, ref long position)
        {
            SkipReferenceHeader(source, ref position);
            return ReadLong(source, ref position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteReference(ByteTable target, ref long position, long value)
        {
            WriteReferenceHeader(target, ref position);
            WriteLong(target, ref position, value);
        }

        #endregion

        #region Read and Write characters

        public string ReadString(ByteTable reader, ref long position, int length)
        {
            Statics.EnsureCharBufferCapacity(length, ref _charBuffer, ignoreContent: true);
            for (int i = 0; i < length; ++i)
                _charBuffer[i] = ReadChar(reader, ref position);
            return new string(_charBuffer, 0, length);
        }

        public void WriteString(ByteTable reader, string value, ref long position)
        {
            foreach (char c in value)
                WriteChar(reader, c, ref position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SkipExpectedCharacter(ByteTable reader, char expectedChar, ref long position)
        {
            var nextChar = ReadChar(reader, ref position);
            if (nextChar != expectedChar)
                throw new Exception();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public char ReadChar(ByteTable reader, ref long position)
        {
            var byte1 = reader.Read(position++);
            var byte2 = reader.Read(position++);
            var converter = new Utils.Converter16(byte1, byte2);
            return converter.AsChar;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteChar(ByteTable writer, char c, ref long position)
        {
            var converter = new Utils.Converter16(c);
            writer.Write(position++, converter.Byte0);
            writer.Write(position++, converter.Byte1);
        }

        /// <summary>
        /// Insert a separator between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertySeparator(ByteTable writer, ref long position)
        {
            WriteChar(writer, ElementSeparator, ref position);
        }

        /// <summary>
        /// Read and skip an expected separator used between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SkipPropertySeparator(ByteTable reader, ref long position)
        {
            SkipExpectedCharacter(reader, ElementSeparator, ref position);
        }

        /// <summary>
        /// Insert a separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteObjectSeparator(ByteTable writer, ref long position)
        {
            writer.Write(position++, (byte)'\r');
            writer.Write(position++, (byte)'\n');
        }

        /// <summary>
        /// Read and skip an expected separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SkipObjectSeparator(ByteTable reader, ref long position)
        {
            char c1 = (char)reader.Read(position++);
            char c2 = (char)reader.Read(position++);

            if (c1 != '\r' || c2 != '\n')
                throw new Exception();
        }

        /// <summary>
        /// Insert a header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteReferenceHeader(ByteTable writer, ref long position)
        {
            WriteChar(writer, ReferenceHeader, ref position);
        }

        /// <summary>
        /// Read and skip an expected header character in front of a reference
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SkipReferenceHeader(ByteTable reader, ref long position)
        {
            SkipExpectedCharacter(reader, ReferenceHeader, ref position);
        }

        #endregion

        #region Private

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ToInt(char b)
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
        private char ToChar(long b)
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
