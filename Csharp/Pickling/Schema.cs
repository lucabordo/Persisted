using Common;
using System;

#if USE_REFLECTION_EMIT
    using System.Reflection.Emit;
#endif

namespace Pickling
{
    /// <summary>
    /// Factory used to generate the schemas that codify the type of stored objects
    /// </summary>
    public static class Schema
    {
        #region Basic types

        /// <summary>
        /// Create a schema component for 8-bit unsigned ints, also known as bytes
        /// </summary>
        public static readonly Schema<byte> Byte = new ByteSchema();

        /// <summary>
        /// Create a schema component for a 32-bit signed integer
        /// </summary>
        public static readonly Schema<int> Int = new IntSchema();

        /// <summary>
        /// Create a schema component for a 64-bit signed integer
        /// </summary>
        public static readonly Schema<long> Long = new LongSchema();

        /// <summary>
        /// Create a schema component for a string
        /// </summary>
        /// <returns></returns>
        public static Schema<string> String()
        {
            return new StringSchema();
        }

        #endregion

        #region Tuples

        /// <summary>
        /// Create a schema component that tuples 2 components
        /// </summary>
        public static Schema<Tuple<T1, T2>> Tuple<T1, T2>(
            Schema<T1> s1, Schema<T2> s2)
        {
            return new TupleSchema<T1, T2>(s1, s2);
        }

        /// <summary>
        /// Create a schema component that tuples 3 components
        /// </summary>
        public static Schema<Tuple<T1, T2, T3>> Tuple<T1, T2, T3>(
            Schema<T1> s1, Schema<T2> s2, Schema<T3> s3)
        {
            return new TupleSchema<T1, T2, T3>(s1, s2, s3);
        }

        /// <summary>
        /// Create a Schema component that tuples 4 components
        /// </summary>
        public static Schema<Tuple<T1, T2, T3, T4>> Tuple<T1, T2, T3, T4>(
            Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4)
        {
            return new TupleSchema<T1, T2, T3, T4>(s1, s2, s3, s4);
        }

        /// <summary>
        /// Create a Schema component that tuples 5 components
        /// </summary>
        public static Schema<Tuple<T1, T2, T3, T4, T5>> Tuple<T1, T2, T3, T4, T5>(
            Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5)
        {
            return new TupleSchema<T1, T2, T3, T4, T5>(s1, s2, s3, s4, s5);
        }

        /// <summary>
        /// Create a Schema component that tuples 6 components
        /// </summary>
        public static Schema<Tuple<T1, T2, T3, T4, T5, T6>> Tuple<T1, T2, T3, T4, T5, T6>(
            Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5, Schema<T6> s6)
        {
            return new TupleSchema<T1, T2, T3, T4, T5, T6>(s1, s2, s3, s4, s5, s6);
        }

        /// <summary>
        /// Create a Schema component that tuples 7 components
        /// </summary>
        public static Schema<Tuple<T1, T2, T3, T4, T5, T6, T7>> Tuple<T1, T2, T3, T4, T5, T6, T7>(
            Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5, Schema<T6> s6, Schema<T7> s7)
        {
            return new TupleSchema<T1, T2, T3, T4, T5, T6, T7>(s1, s2, s3, s4, s5, s6, s7);
        }

        #endregion

        #region Arrays

        /// <summary>
        /// Create a Schema component for arrays of a certain (serializable) type
        /// </summary>
        public static Schema<T[]> Array<T>(Schema<T> s)
        {
            return new InlineArray<T>(s);
        }

        #endregion
    }


    /// <summary>
    /// A schema for entries that can be stored in collections
    /// </summary>
    public abstract class Schema<T>
    {
        /// <summary>
        /// True if the result of <code>GetDynamicSize(x)</code> is independent of <value>x</value>
        /// </summary>
        internal abstract bool IsFixedSize { get; }

        /// <summary>
        /// Number of bytes of storage used by a specific element
        /// </summary>
        internal abstract int GetDynamicSize(T element);

        /// <summary>
        /// Extract an element of the corresponding type from a storage composed of a fixed and a dynamic part
        /// </summary>
        internal abstract T Read(ByteBufferReadCursor segment);

        /// <summary>
        /// Insert an element of the corresponding type into a storage composed of a fixed and a dynamic part
        /// </summary>
        internal abstract void Write(ByteBufferWriteCursor segment, T element);
    }
}
