﻿using System;
using Common;
using System.Diagnostics.Contracts;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    /// <summary>
    /// A schema component for generic arrays;
    /// Non-resizable arrays of arbitrary, non-homogeneous sizes
    /// </summary>
    internal class InlineArray<T> : Schema<T[]>
    {
        readonly Schema<T> innerSchema;

        public InlineArray(Schema<T> schema)
        {
            innerSchema = schema;
        }

        internal override bool IsFixedSize
        {
            get { return false; }
        }

        internal override int GetDynamicSize(T[] element)
        {
            int size = Encoding.EncodingSizeForInt;

            foreach (var entry in element)
            {
                size += innerSchema.GetDynamicSize(entry);
            }

            return size;
        }

        internal override T[] Read(ByteSegmentReadView segment)
        {
            int size = Encoding.ReadInt(segment);
            var result = new T[size];

            for (int i = 0; i < size; ++i)
            {
                result[i] = innerSchema.Read(segment);
            }

            return result;
        }

        internal override void Write(ByteSegmentWriteView segment, T[] element)
        {
            // TODO: add marker? e.g. [size]
            Encoding.WriteInt(segment, element.Length);

            foreach (var entry in element)
            {
                innerSchema.Write(segment, entry);
            }
        }
    }


    /// <summary>
    /// A schema component for generic arrays;
    /// Arrays of a fixed, homogeneous size
    /// </summary>
    internal class FixedSizeInlineArray<T> : InlineArray<T>
    {
        readonly int size;

        internal override bool IsFixedSize
        {
            get { return true; }
        }

        public FixedSizeInlineArray(Schema<T> schema, int size):
            base(schema)
        {
            this.size = size;
        }

        internal override void Write(ByteSegmentWriteView segment, T[] element)
        {
            if (element.Length != size)
                throw new ArgumentException("");
            base.Write(segment, element);
        }
    }
    

    /// <summary>
    /// A schema component that tuples 1 component
    /// </summary>
    internal class TupleSchema<T1> : Schema<Tuple<T1>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;

        internal TupleSchema(Schema<T1> s1)
        {
            S1 = s1;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get { return S1.IsFixedSize; }
        }

        internal override int GetDynamicSize(Tuple<T1> element)
        {
            return S1.GetDynamicSize(element.Item1);
        }

        internal override Tuple<T1> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            return Tuple.Create(i1);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1> element)
        {
            S1.Write(segment, element.Item1);
        }

        #endregion
    }


    /// <summary>
    /// A schema component that tuples 2 components
    /// </summary>
    internal class TupleSchema<T1, T2> : Schema<Tuple<T1, T2>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2)
        {
            S1 = s1;
            S2 = s2;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get { return S1.IsFixedSize && S2.IsFixedSize; }
        }

        internal override int GetDynamicSize(Tuple<T1, T2> element)
        {
            return TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2);
        }

        internal override Tuple<T1, T2> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);

            return Tuple.Create(i1, i2);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
        }

        #endregion
    }

    /// <summary>
    /// A schema component that tuples 3 components
    /// </summary>
    internal class TupleSchema<T1, T2, T3> : Schema<Tuple<T1, T2, T3>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;
        internal readonly Schema<T3> S3;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2, Schema<T3> s3)
        {
            S1 = s1;
            S2 = s2;
            S3 = s3;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get { return S1.IsFixedSize && S2.IsFixedSize && S3.IsFixedSize; }
        }
        
        internal override int GetDynamicSize(Tuple<T1, T2, T3> element)
        {
            return 2 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) + S3.GetDynamicSize(element.Item3);
        }

        internal override Tuple<T1, T2, T3> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i3 = S3.Read(segment);
            return Tuple.Create(i1, i2, i3);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2, T3> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
            TupleSchemaHelpers.WriteSeparator(segment);
            S3.Write(segment, element.Item3);
        }

        #endregion
    }

    /// <summary>
    /// A schema component that tuples 4 components
    /// </summary>
    internal class TupleSchema<T1, T2, T3, T4> : Schema<Tuple<T1, T2, T3, T4>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;
        internal readonly Schema<T3> S3;
        internal readonly Schema<T4> S4;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4)
        {
            S1 = s1;
            S2 = s2;
            S3 = s3;
            S4 = s4;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get
            {
                return S1.IsFixedSize && S2.IsFixedSize 
                    && S3.IsFixedSize && S4.IsFixedSize;
            }
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4> element)
        {
            return 3 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) +
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4);
        }

        internal override Tuple<T1, T2, T3, T4> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i3 = S3.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i4 = S4.Read(segment);

            return Tuple.Create(i1, i2, i3, i4);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2, T3, T4> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
            TupleSchemaHelpers.WriteSeparator(segment);
            S3.Write(segment, element.Item3);
            TupleSchemaHelpers.WriteSeparator(segment);
            S4.Write(segment, element.Item4);
        }

        #endregion
    }

    /// <summary>
    /// A schema component that tuples 5 components
    /// </summary>
    internal class TupleSchema<T1, T2, T3, T4, T5> : Schema<Tuple<T1, T2, T3, T4, T5>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;
        internal readonly Schema<T3> S3;
        internal readonly Schema<T4> S4;
        internal readonly Schema<T5> S5;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5)
        {
            S1 = s1;
            S2 = s2;
            S3 = s3;
            S4 = s4;
            S5 = s5;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get
            {
                return S1.IsFixedSize && S2.IsFixedSize 
                    && S3.IsFixedSize && S4.IsFixedSize 
                    && S5.IsFixedSize;
            }
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5> element)
        {
            return 4 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) +
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) +
                S5.GetDynamicSize(element.Item5);
        }

        internal override Tuple<T1, T2, T3, T4, T5> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i3 = S3.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i4 = S4.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i5 = S5.Read(segment);

            return Tuple.Create(i1, i2, i3, i4, i5);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2, T3, T4, T5> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
            TupleSchemaHelpers.WriteSeparator(segment);
            S3.Write(segment, element.Item3);
            TupleSchemaHelpers.WriteSeparator(segment);
            S4.Write(segment, element.Item4);
            TupleSchemaHelpers.WriteSeparator(segment);
            S5.Write(segment, element.Item5);
        }

        #endregion
    }

    /// <summary>
    /// A schema component that tuples 6 components
    /// </summary>
    internal class TupleSchema<T1, T2, T3, T4, T5, T6> : Schema<Tuple<T1, T2, T3, T4, T5, T6>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;
        internal readonly Schema<T3> S3;
        internal readonly Schema<T4> S4;
        internal readonly Schema<T5> S5;
        internal readonly Schema<T6> S6;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5, Schema<T6> s6)
        {
            S1 = s1;
            S2 = s2;
            S3 = s3;
            S4 = s4;
            S5 = s5;
            S6 = s6;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get
            {
                return S1.IsFixedSize && S2.IsFixedSize
                    && S3.IsFixedSize && S4.IsFixedSize
                    && S5.IsFixedSize && S6.IsFixedSize;
            }
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5, T6> element)
        {
            return 5 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) +
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) +
                S5.GetDynamicSize(element.Item5) + S6.GetDynamicSize(element.Item6);
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i3 = S3.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i4 = S4.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i5 = S5.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i6 = S6.Read(segment);

            return Tuple.Create(i1, i2, i3, i4, i5, i6);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2, T3, T4, T5, T6> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
            TupleSchemaHelpers.WriteSeparator(segment);
            S3.Write(segment, element.Item3);
            TupleSchemaHelpers.WriteSeparator(segment);
            S4.Write(segment, element.Item4);
            TupleSchemaHelpers.WriteSeparator(segment);
            S5.Write(segment, element.Item5);
            TupleSchemaHelpers.WriteSeparator(segment);
            S6.Write(segment, element.Item6);
        }

        #endregion
    }

    /// <summary>
    /// A schema component that tuples 7 components
    /// </summary>
    internal class TupleSchema<T1, T2, T3, T4, T5, T6, T7> : Schema<Tuple<T1, T2, T3, T4, T5, T6, T7>>
    {
        #region Fields and Construction

        internal readonly Schema<T1> S1;
        internal readonly Schema<T2> S2;
        internal readonly Schema<T3> S3;
        internal readonly Schema<T4> S4;
        internal readonly Schema<T5> S5;
        internal readonly Schema<T6> S6;
        internal readonly Schema<T7> S7;

        internal TupleSchema(Schema<T1> s1, Schema<T2> s2, Schema<T3> s3, Schema<T4> s4, Schema<T5> s5, Schema<T6> s6, Schema<T7> s7)
        {
            S1 = s1;
            S2 = s2;
            S3 = s3;
            S4 = s4;
            S5 = s5;
            S6 = s6;
            S7 = s7;
        }

        #endregion

        #region Methods

        internal override bool IsFixedSize
        {
            get
            {
                return S1.IsFixedSize && S2.IsFixedSize
                    && S3.IsFixedSize && S4.IsFixedSize
                    && S5.IsFixedSize && S6.IsFixedSize
                    && S7.IsFixedSize;
            }
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5, T6, T7> element)
        {
            return 6 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) +
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) +
                S5.GetDynamicSize(element.Item5) + S6.GetDynamicSize(element.Item6) +
                S7.GetDynamicSize(element.Item7);
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6, T7> Read(ByteSegmentReadView segment)
        {
            var i1 = S1.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i2 = S2.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i3 = S3.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i4 = S4.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i5 = S5.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i6 = S6.Read(segment);
            TupleSchemaHelpers.SkipSeparators(segment);
            var i7 = S7.Read(segment);

            return Tuple.Create(i1, i2, i3, i4, i5, i6, i7);
        }

        internal override void Write(ByteSegmentWriteView segment, Tuple<T1, T2, T3, T4, T5, T6, T7> element)
        {
            S1.Write(segment, element.Item1);
            TupleSchemaHelpers.WriteSeparator(segment);
            S2.Write(segment, element.Item2);
            TupleSchemaHelpers.WriteSeparator(segment);
            S3.Write(segment, element.Item3);
            TupleSchemaHelpers.WriteSeparator(segment);
            S4.Write(segment, element.Item4);
            TupleSchemaHelpers.WriteSeparator(segment);
            S5.Write(segment, element.Item5);
            TupleSchemaHelpers.WriteSeparator(segment);
            S6.Write(segment, element.Item6);
            TupleSchemaHelpers.WriteSeparator(segment);

            S7.Write(segment, element.Item7);
        }

        #endregion
    }

    static internal class TupleSchemaHelpers
    {
        internal static int FixedSeparatorSize = Encoding.EncodingSizeForElementSeparator;
        internal static int DynamicSeparatorSize = Encoding.EncodingSizeForElementSeparator;

        internal static void WriteSeparator(ByteSegmentWriteView segment)
        {
            Encoding.WritePropertySeparator(segment);
            Encoding.WriteObjectSeparator(segment);
        }

        internal static void SkipSeparators(ByteSegmentReadView segment)
        {
            Encoding.SkipPropertySeparator(segment);
            Encoding.SkipObjectSeparator(segment);
        }
    }
}
