using System;
using System.Collections.Generic;
using Persisted.Typed;
using System.Diagnostics;

namespace Persisted
{
    /// <summary>
    /// Factory used to generate the schemas that codify the type of stored objects
    /// </summary>
    internal static class Schema
    {
        #region Basic types

        // Note that these objects have a bit of state, so it makes sense
        // to allocate a single copy of them.
        // Note also that we don't support multi-threading here

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
        /// Create a schema component for a string of arbitrary size
        /// </summary>
        public static readonly Schema<string> String = new StringSchema();

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
    }


    /// <summary>
    /// A schema for entries that can be stored in collections
    /// </summary>
    internal abstract class Schema<T>
    {
        #region Abstract Properties and methods 

        /// <summary>
        /// Number of bytes used for each instance of <see cref="T"/> in the primary storage
        /// </summary>
        internal abstract int GetSize(Encoding encoding);

        /// <summary>
        /// True if the represented type is Fixed-size. 
        /// In this case we don't need to allocate an auxiliary storage
        /// </summary>
        internal virtual bool IsFixedSized { get { return true; } }

        /// <summary>
        /// Extract an element of the corresponding type from a source.
        /// </summary>
        /// <param name="image">A group of byte tables that encodes a table of <typeparamref name="T"/></param>
        /// <param name="position">The position at which the encoding value starts</param>
        internal abstract T Read(TableByteRepresentation image, Encoding encoding, ref long position);

        /// <summary>
        /// Insert an element of the corresponding type into a target.
        /// </summary>
        /// <param name="image">A group of byte tables that encodes a table of <typeparamref name="T"/></param>
        /// <param name="position">The position at which the encoding value starts</param>
        /// <param name="element">A value to write</param>
        internal abstract void Write(TableByteRepresentation image, Encoding encoding, ref long position, T element);

        #endregion

        #region Non-abstract utilities 

        /// <summary>
        /// Read a sequence of tuples of the specified length
        /// starting from the specified position
        /// </summary>
        internal IEnumerable<T> Read(TableByteRepresentation image, Encoding encoding, long startPosition, int count)
        {
            long position = startPosition;
            var primary = image.PrimaryContainer;

            for (int i = 0; i < count; ++i)
            {
                if (i > 0)
                    encoding.SkipObjectSeparator(primary, ref position);
                yield return Read(image, encoding, ref position);
            }
        }

        /// <summary>
        /// Write an array of tuples 
        /// starting from the specified position
        /// </summary>
        internal void Write(TableByteRepresentation image, Encoding encoding, long startPosition, params T[] elements)
        {
            int i = -1;
            long position = startPosition;
            var primary = image.PrimaryContainer;

            foreach (var element in elements)
            {
                if (++i > 0)
                    encoding.WriteObjectSeparator(primary, ref position);
                Write(image, encoding, ref position, element);
            }
        }

        #endregion
    }
}
