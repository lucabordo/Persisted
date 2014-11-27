using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Persisted.Utils
{
    /// <summary>
    /// Well-typed boxing
    /// </summary>
    public class Ref<T> 
        where T: struct
    {
        public T Value { get; private set; }

        public Ref(T value)
        {
            Value = value;
        }
    }


    /// <summary>
    /// Some static methods that require instantiations per generic type
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public static class Statics<T>
    {
        /// <summary>
        /// A terminated task whose result is default
        /// </summary>
        public static readonly Task<T> ZeroTask = Task.FromResult(default(T));

        /// <summary>
        /// An empty array of length 0
        /// </summary>
        public static readonly T[] EmptyArray = new T[0];

        /// <summary>
        /// A terminated task whose result is an empty array
        /// </summary>
        public static readonly Task<T[]> EmptyArrayTask = Task.FromResult(EmptyArray);
    }


    /// <summary>
    /// Some useful static methods 
    /// </summary>
    public static class Statics
    {
        /// <summary>
        /// A terminated Task with no result
        /// </summary>
        public static readonly Task ZeroTask = Task.Run(() => { });

        /// <summary>
        /// Maximal number of digits for 32-bit integers
        /// </summary>
        public static readonly int Int32Size = Int32.MinValue.ToString().Length;

        /// <summary>
        /// Maximal number of digits for 64-bit integers
        /// </summary>
        public static readonly int Int64Size = Int64.MinValue.ToString().Length;
        
        /// <summary>
        /// Exchange the two elements 
        /// </summary>
        public static void Swap<T>(ref T a, ref T b)
        {
            T olda = a;
            a = b;
            b = olda;
        }

        /// <summary>
        /// Enumerate <paramref name="count"/> elements from <paramref name="start"/>
        /// </summary>
        public static IEnumerable<long> LongRange(long start, long count)
        {
            for (long i = 0; i < count; ++i)
            {
                yield return start + i;
            }
        }

        /// <summary>
        /// Run a function - typically as a static initializer
        /// </summary>
        public static T Run<T>(Func<T> fun)
        {
            return fun();
        }

        /// <summary>
        /// Reallocate an array with double size if not 
        /// </summary>
        public static void EnsureCharBufferCapacity<T>(int capacity, ref T[] array, bool ignoreContent = false)
        {
            Debug.Assert(array.Length > 0);

            if (array.Length < capacity)
            {
                int newCapacity = array.Length;
                while (newCapacity < capacity)
                    newCapacity *= 2;
                var old = array;
                array = new T[newCapacity];
                if (!ignoreContent)
                {
                    Array.Copy(old, array, old.Length);
                }
            }
        }
    }


    /// <summary>
    /// An enumerator that wraps an existing enumerable
    /// </summary>
    /// <remarks>
    /// Just because sometimes writing the enumerator is easier
    /// </remarks>
    public class EnumerableFromEnumerator<T> : IEnumerable<T>
    {
        private readonly IEnumerator<T> _enumerator;

        public EnumerableFromEnumerator(IEnumerator<T> enumerator)
        {
            _enumerator = enumerator;
        }

        public IEnumerator<T> GetEnumerator()
        {
            _enumerator.Reset();
            return _enumerator;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _enumerator;
        }
    }


    /// <summary>
    /// A converter between various 16-bit value types 
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Converter16
    {
        [FieldOffset(0)]
        public char AsChar;

        [FieldOffset(0)]
        public byte Byte0;

        [FieldOffset(1)]
        public byte Byte1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Converter16(byte byte0, byte byte1)
        {
            AsChar = (char)0;
            Byte0 = byte0;
            Byte1 = byte1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Converter16(char c)
        {
            Byte0 = 0;
            Byte1 = 0;
            AsChar = c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static char FromBytes(byte byte0, byte byte1)
        {
            var converter = new Converter16(byte0, byte1);
            return converter.AsChar;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ToBytes(char c, out byte byte0, out byte byte1)
        {
            var converter = new Converter16(c);
            byte0 = converter.Byte0;
            byte1 = converter.Byte1;
        }
    }


    /// <summary>
    /// A converter between various 32-bit value types 
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Converter32
    {
        [FieldOffset(0)]
        public Int32 AsInt;

        [FieldOffset(0)]
        public byte Byte0;

        [FieldOffset(1)]
        public byte Byte1;

        [FieldOffset(2)]
        public byte Byte2;

        [FieldOffset(3)]
        public byte Byte3;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Converter32(byte byte0, byte byte1, byte byte2, byte byte3)
        {
            AsInt = 0;
            Byte0 = byte0;
            Byte1 = byte1;
            Byte2 = byte2;
            Byte3 = byte3;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Converter32(int value)
        {
            Byte0 = 0;
            Byte1 = 0;
            Byte2 = 0;
            Byte3 = 0;
            AsInt = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FromBytes(byte byte0, byte byte1, byte byte2, byte byte3)
        {
            var converter = new Converter32(byte0, byte1, byte2, byte3);
            return converter.AsInt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ToBytes(int value, out byte byte0, out byte byte1, out byte byte2, out byte byte3)
        {
            var converter = new Converter32(value);
            byte0 = converter.Byte0;
            byte1 = converter.Byte1;
            byte2 = converter.Byte2;
            byte3 = converter.Byte3;
        }
    }


    /// <summary>
    /// Conversions between various 64-bit value types
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Converter64
    {
        [FieldOffset(0)]
        public double AsDouble;

        [FieldOffset(0)]
        public long AsLong;

        [FieldOffset(0)]
        public byte Byte0;
        [FieldOffset(1)]
        public byte Byte1;
        [FieldOffset(2)]
        public byte Byte2;
        [FieldOffset(3)]
        public byte Byte3;
        [FieldOffset(4)]
        public byte Byte4;
        [FieldOffset(5)]
        public byte Byte5;
        [FieldOffset(6)]
        public byte Byte6;
        [FieldOffset(7)]
        public byte Byte7;
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Converter64(byte byte0, byte byte1, byte byte2, byte byte3, byte byte4, byte byte5, byte byte6, byte byte7)
        {
            AsDouble = 0;
            AsLong = 0;
            Byte0 = byte0;
            Byte1 = byte1;
            Byte2 = byte2;
            Byte3 = byte3;
            Byte4 = byte4;
            Byte5 = byte5;
            Byte6 = byte6;
            Byte7 = byte7;
        }

        /// <summary>
        /// Convert a long into a double that has the same 64 bits
        /// </summary>
        public static double DoubleFromLong(long encoding)
        {
            var converter = new Converter64();
            converter.AsLong = encoding;
            return converter.AsDouble;
        }

        /// <summary>
        /// Convert a double into a long that has the same 64 bits
        /// </summary>
        public static long LongFromDouble(double encoding)
        {
            var converter = new Converter64();
            converter.AsDouble = encoding;
            return converter.AsLong;
        }

        /// <summary>
        /// Convert a long into a double that has the same 64 bits
        /// </summary>
        public static DateTime DateTimeFromLong(long encoding)
        {
            return DateTime.FromBinary(encoding);
        }

        /// <summary>
        /// Convert a double into a long that has the same 64 bits
        /// </summary>
        public static long LongFromDateTime(DateTime encoding)
        {
            return encoding.ToBinary();
        }
    }
}
