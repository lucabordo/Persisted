using Common;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Pickling
{
    public interface IEncoding
    {
        int EncodingSizeForByte { get; }
        int EncodingSizeForInt { get; }
        int EncodingSizeForLong { get; }

        long ReadIntegralType(ByteBufferReadCursor source, int digits);
        void WriteIntegralType(ByteBufferWriteCursor target, long value, int digits);

        /// <summary>
        /// Size for encoding a string in dynamic storage
        /// </summary>
        int EncodingSizeForString(int stringLength);

        string ReadString(ByteBufferReadCursor source, int length, ref char[] charBuffer);

        void WriteString(ByteBufferWriteCursor writer, string value);

        char ReadChar(ByteBufferReadCursor source);
        void WriteChar(ByteBufferWriteCursor writer, char c);

        // separators may not require writing a char at all. Just a bool function? 
        char ReadSeparator(ByteBufferReadCursor source);
        void WriteSeparator(ByteBufferWriteCursor writer, char c);
    }

    public static class Examples
    {
        public static void DoSomething<ConcreteEncodingType>(ConcreteEncodingType encoding)
            where ConcreteEncodingType : struct, IEncoding
        {

        }
    }


    public class ReadableEncodingStatic : IEncoding
    {
        public int EncodingSizeForByte
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int EncodingSizeForInt
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int EncodingSizeForLong
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int EncodingSizeForString(int stringLength)
        {
            throw new NotImplementedException();
        }

        public char ReadChar(ByteBufferReadCursor source)
        {
            throw new NotImplementedException();
        }

        public long ReadIntegralType(ByteBufferReadCursor source, int digits)
        {
            throw new NotImplementedException();
        }

        public char ReadSeparator(ByteBufferReadCursor source)
        {
            throw new NotImplementedException();
        }

        public string ReadString(ByteBufferReadCursor source, int length, ref char[] charBuffer)
        {
            throw new NotImplementedException();
        }

        public void WriteChar(ByteBufferWriteCursor writer, char c)
        {
            throw new NotImplementedException();
        }

        public void WriteIntegralType(ByteBufferWriteCursor target, long value, int digits)
        {
            throw new NotImplementedException();
        }

        public void WriteSeparator(ByteBufferWriteCursor writer, char c)
        {
            throw new NotImplementedException();
        }

        public void WriteString(ByteBufferWriteCursor writer, string value)
        {
            throw new NotImplementedException();
        }
    }


    /// <summary>
    /// Low-level methods that define how numerical and basic types and encoded into bytes.
    /// </summary>
    /// <remarks>
    /// So far we have a single class - but the design should make it easy to move to interface 
    /// and various encodings if we need to decide encoding dynamically.
    /// Not that encoding objects are NOT THREAD-SAFE so one instance per thread should be created
    /// </remarks>
    public static class ReadableEncoding
    {
        #region Read and Write numbers

        private static readonly int CharCountForByte = byte.MaxValue.ToString().Length;
        private static readonly int CharCountForInt = int.MinValue.ToString().Length;
        private static readonly int CharCountForLong = long.MinValue.ToString().Length;

        public static readonly int EncodingSizeForByte = 2 * CharCountForByte;
        public static readonly int EncodingSizeForInt = 2 * CharCountForInt;
        public static readonly int EncodingSizeForLong = 2 * CharCountForLong;

        public static byte ReadByte(ByteBufferReadCursor source)
        {
            long result = ReadIntegralType(source, CharCountForByte);
            return (byte)result;
        }
        
        public static int ReadInt(ByteBufferReadCursor source)
        {
            long result = ReadIntegralType(source, CharCountForInt);
            return (int)result;
        }
        
        public static long ReadLong(ByteBufferReadCursor source)
        {
            long result = ReadIntegralType(source, CharCountForLong);
            return result;
        }

        public static void WriteByte(ByteBufferWriteCursor target, byte value)
        {
            WriteIntegralType(target, value, CharCountForByte);
        }

        public static void WriteInt(ByteBufferWriteCursor target, int value)
        {
            WriteIntegralType(target, value, CharCountForInt);
        }

        public static void WriteLong(ByteBufferWriteCursor target, long value)
        {
            WriteIntegralType(target, value, CharCountForLong);
        }

        private static long ReadIntegralType(ByteBufferReadCursor source, int digits)
        {
            Debug.Assert(-long.MaxValue == long.MinValue + 1);
            // Note that we are assuming the following min and max values 
            //  9223372036854775807;
            // -9223372036854775808;
            // We have more negative values than positive ones. 
            // To read -9223372036854775808 correctly we need to read everything negated

            // Locate first non-space
            int i = 0;
            char currentChar;

            for (; ; ++i)
            {
                currentChar = ReadChar(source);

                if (currentChar != ' ')
                    break;
                if (i >= digits)
                    throw new Exception();
            }

            bool negative = (currentChar == '-');
            long minusResults = negative ? 0 : -ToInt(currentChar);
            
            // remaining spaces
            for (++i; i < digits; ++i)
            {
                currentChar = ReadChar(source);
                minusResults = (10 * minusResults) - ToInt(currentChar);
            }

            return negative ? minusResults : (-minusResults);
        }

        private static void WriteIntegralType(ByteBufferWriteCursor target, long value, int digits)
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
                converter.AsChar = ToChar(-(v % 10));
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            // Precede this text by a sign if needed and (backwards) by spaces

            if (negative)
            {
                converter.AsChar = '-';
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            converter.AsChar = ' ';
            while (byteId != 0)
            {
                target[--byteId] = converter.Byte1;
                target[--byteId] = converter.Byte0;
            }

            target.MoveForward(2 * digits);
        }

        #endregion

        #region Read and write offsets and references

        internal const char ReferenceHeader = '*';
        internal const char OffsetHeader = '@';
        public static readonly int EncodingSizeForOffset = EncodingSizeForChar + EncodingSizeForInt;
        public static readonly int EncodingSizeForReference = EncodingSizeForChar + EncodingSizeForLong;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ReadOffset(ByteBufferReadCursor reader)
        {
            SkipExpectedCharacter(reader, OffsetHeader);
            return ReadInt(reader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteOffset(ByteBufferWriteCursor writer, int value)
        {
            WriteChar(writer, OffsetHeader);
            WriteInt(writer, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadReference(ByteBufferReadCursor reader)
        {
            SkipExpectedCharacter(reader, ReferenceHeader);
            return ReadLong(reader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteReference(ByteBufferWriteCursor writer, long value)
        {
            WriteChar(writer, ReferenceHeader);
            WriteLong(writer, value);
        }

        #endregion

        #region Read and Write strings

        // TODO add some annotations - perhaps just "quotes"

        /// <summary>
        /// Size for encoding a string in dynamic storage
        /// </summary>
        public static int EncodingSizeForString(int stringLength)
        {
            return stringLength * EncodingSizeForChar;
        }

        public static string ReadString(ByteBufferReadCursor source, int length, ref char[] charBuffer)
        {
            while (charBuffer.Length < length)
                charBuffer = new char[charBuffer.Length * 2];
            for (int i = 0; i < length; ++i)
                charBuffer[i] = ReadChar(source);
            return new string(charBuffer, 0, length);
        }

        public static void WriteString(ByteBufferWriteCursor writer, string value)
        {
            foreach (char c in value)
                WriteChar(writer, c);
        }

        #endregion

        #region Read and write characters

        public static readonly int EncodingSizeForChar = 2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static char ReadChar(ByteBufferReadCursor source)
        {
            var byte0 = source.NextChar;
            var byte1 = source.NextChar;
            var converter = new Converter16(byte0, byte1);
            return converter.AsChar;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteChar(ByteBufferWriteCursor writer, char c)
        {
            var converter = new Converter16(c);
            writer.NextChar = converter.Byte0;
            writer.NextChar = converter.Byte1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipExpectedCharacter(ByteBufferReadCursor source, char expectedChar)
        {
            var nextChar = ReadChar(source);
            if (nextChar != expectedChar)
                throw new Exception();
        }

        #endregion

        #region Read and write separators and Indicators

        // Note that indicators and separators are purely here for readability 
        // of the encoding, no parsing should rely on them.

        // Lots of lines for not much here but this has good readability 
        // and allows for variable size indicators and separators

        // ARRAYS

        public static readonly int EncodingSizeForArrayStartIndicator = EncodingSizeForChar;
        internal const char ArrayStartIndicator = '[';

        /// <summary>
        /// Insert an indicator of start of an array
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArrayStartIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, ArrayStartIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the start of an array
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipArrayStartIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, ArrayStartIndicator);
        }

        public static readonly int EncodingSizeForArrayEndIndicator = EncodingSizeForChar;
        internal const char ArrayEndIndicator = ']';

        /// <summary>
        /// Insert an indicator of end of an array
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArrayEndIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, ArrayEndIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the end of an array
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipArrayEndIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, ArrayEndIndicator);
        }

        // TUPLES

        public static readonly int EncodingSizeForTupleStartIndicator = EncodingSizeForChar;
        internal const char TupleStartIndicator = '(';

        /// <summary>
        /// Insert an indicator of start of a tuple
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteTupleStartIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, TupleStartIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the start of a tuple
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipTupleStartIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, TupleStartIndicator);
        }

        public static readonly int EncodingSizeForTupleEndIndicator = EncodingSizeForChar;
        internal const char TupleEndIndicator = ')';

        /// <summary>
        /// Insert an indicator of end of a tuple
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteTupleEndIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, TupleEndIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the end of a tuple
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipTupleEndIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, TupleEndIndicator);
        }

        // STRINGS

        public static readonly int EncodingSizeForStringStartIndicator = EncodingSizeForChar;
        internal const char StringStartIndicator = '"';

        /// <summary>
        /// Insert an indicator of start of a string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteStringStartIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, StringStartIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the start of a string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipStringStartIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, StringStartIndicator);
        }

        public static readonly int EncodingSizeForStringEndIndicator = EncodingSizeForChar;
        internal const char StringEndIndicator = '"';

        /// <summary>
        /// Insert an indicator of End of a string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteStringEndIndicator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, StringEndIndicator);
        }

        /// <summary>
        /// Read and skip an expected indicator used at the end of a string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipStringEndIndicator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, StringEndIndicator);
        }

        // SEPARATORS

        public static readonly int EncodingSizeForElementSeparator = EncodingSizeForChar;
        internal const char ElementSeparator = ',';

        /// <summary>
        /// Insert a separator between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WritePropertySeparator(ByteBufferWriteCursor writer)
        {
            WriteChar(writer, ElementSeparator);
        }

        /// <summary>
        /// Read and skip an expected separator used between the encoding of two properties of a object 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipPropertySeparator(ByteBufferReadCursor source)
        {
            SkipExpectedCharacter(source, ElementSeparator);
        }

        public static readonly int EncodingSizeForObjectSeparator = 2 * EncodingSizeForChar;

        /// <summary>
        /// Insert a separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteObjectSeparator(ByteBufferWriteCursor writer)
        {
            writer.NextChar = (byte)'\r';
            writer.NextChar = (byte)'\n';
        }

        /// <summary>
        /// Read and skip an expected separator between the encodings of two objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SkipObjectSeparator(ByteBufferReadCursor source)
        {
            char c0 = (char)source.NextChar;
            char c1 = (char)source.NextChar;

            if (c0 != '\r' || c1 != '\n')
                throw new Exception();
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
