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
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForByte;
        }

        internal override int GetDynamicSize(byte element)
        {
            return 0;
        }

        internal override byte Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            return Encoding.ReadByte(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, byte element)
        {
            Encoding.WriteByte(fixedStorage, element);
        }
    }

    /// <summary>
    /// A schema component for classical, 32-bit integers
    /// </summary>
    internal class IntSchema : Schema<int>
    {
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForInt;
        }

        internal override int GetDynamicSize(int element)
        {
            return 0;
        }

        internal override int Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            return Encoding.ReadInt(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, int element)
        {
            Encoding.WriteInt(fixedStorage, element);
        }
    }

    /// <summary>
    /// A schema component for long, 64-bit integers
    /// </summary>
    internal class LongSchema : Schema<long>
    {
        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForLong;
        }

        internal override int GetDynamicSize(long element)
        {
            return 0;
        }

        internal override long Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            return Encoding.ReadLong(fixedStorage);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, long element)
        {
            Encoding.WriteLong(fixedStorage, element);
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

        internal override int GetFixedSize()
        {
            return S1.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1> element)
        {
            return S1.GetDynamicSize(element.Item1);
        }

        internal override Tuple<T1> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
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

        internal override int GetFixedSize()
        {
            return TupleSchemaHelpers.FixedSeparatorSize + 
                S1.GetFixedSize() + S2.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2> element)
        {
            return TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2);
        }

        internal override Tuple<T1, T2> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
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

        internal override int GetFixedSize()
        {
            return 2 * TupleSchemaHelpers.FixedSeparatorSize +
                S1.GetFixedSize() + S2.GetFixedSize() + S3.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3> element)
        {
            return  2 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) + S3.GetDynamicSize(element.Item3);
        }

        internal override Tuple<T1, T2, T3> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i3 = S3.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2, i3);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2, T3> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S3.Write(fixedStorage, dynamicStorage, element.Item3);
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

        internal override int GetFixedSize()
        {
            return 3 * TupleSchemaHelpers.FixedSeparatorSize +
                S1.GetFixedSize() + S2.GetFixedSize() +
                S3.GetFixedSize() + S4.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4> element)
        {
            return 3 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) +
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4);
        }

        internal override Tuple<T1, T2, T3, T4> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i3 = S3.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i4 = S4.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2, i3, i4);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2, T3, T4> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S3.Write(fixedStorage, dynamicStorage, element.Item3);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S4.Write(fixedStorage, dynamicStorage, element.Item4);
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

        internal override int GetFixedSize()
        {
            return 4 * TupleSchemaHelpers.FixedSeparatorSize +
                S1.GetFixedSize() + S2.GetFixedSize() + S3.GetFixedSize() +
                S4.GetFixedSize() + S5.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5> element)
        {
            return 4 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) + 
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) + 
                S5.GetDynamicSize(element.Item5);
        }

        internal override Tuple<T1, T2, T3, T4, T5> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i3 = S3.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i4 = S4.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i5 = S5.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2, i3, i4, i5);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2, T3, T4, T5> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S3.Write(fixedStorage, dynamicStorage, element.Item3);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S4.Write(fixedStorage, dynamicStorage, element.Item4);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S5.Write(fixedStorage, dynamicStorage, element.Item5);
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

        internal override int GetFixedSize()
        {
            return 5 * TupleSchemaHelpers.FixedSeparatorSize +
                S1.GetFixedSize() + S2.GetFixedSize() + S3.GetFixedSize() +
                S4.GetFixedSize() + S5.GetFixedSize() + S6.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5, T6> element)
        {
            return 5 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) + 
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) + 
                S5.GetDynamicSize(element.Item5) + S6.GetDynamicSize(element.Item6);
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i3 = S3.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i4 = S4.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i5 = S5.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i6 = S6.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2, i3, i4, i5, i6);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2, T3, T4, T5, T6> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S3.Write(fixedStorage, dynamicStorage, element.Item3);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S4.Write(fixedStorage, dynamicStorage, element.Item4);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S5.Write(fixedStorage, dynamicStorage, element.Item5);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S6.Write(fixedStorage, dynamicStorage, element.Item6);
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

        internal override int GetFixedSize()
        {
            return 6 * TupleSchemaHelpers.FixedSeparatorSize +
                S1.GetFixedSize() + S2.GetFixedSize() + S3.GetFixedSize() + S4.GetFixedSize() +
                S5.GetFixedSize() + S6.GetFixedSize() + S7.GetFixedSize();
        }

        internal override int GetDynamicSize(Tuple<T1, T2, T3, T4, T5, T6, T7> element)
        {
            return 6 * TupleSchemaHelpers.DynamicSeparatorSize +
                S1.GetDynamicSize(element.Item1) + S2.GetDynamicSize(element.Item2) + 
                S3.GetDynamicSize(element.Item3) + S4.GetDynamicSize(element.Item4) + 
                S5.GetDynamicSize(element.Item5) + S6.GetDynamicSize(element.Item6) +
                S7.GetDynamicSize(element.Item7);
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6, T7> Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            var i1 = S1.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i2 = S2.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i3 = S3.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i4 = S4.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i5 = S5.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i6 = S6.Read(fixedStorage, dynamicStorage);
            TupleSchemaHelpers.SkipSeparators(fixedStorage, dynamicStorage);
            var i7 = S7.Read(fixedStorage, dynamicStorage);
            return Tuple.Create(i1, i2, i3, i4, i5, i6, i7);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Tuple<T1, T2, T3, T4, T5, T6, T7> element)
        {
            S1.Write(fixedStorage, dynamicStorage, element.Item1);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S2.Write(fixedStorage, dynamicStorage, element.Item2);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S3.Write(fixedStorage, dynamicStorage, element.Item3);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S4.Write(fixedStorage, dynamicStorage, element.Item4);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S5.Write(fixedStorage, dynamicStorage, element.Item5);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S6.Write(fixedStorage, dynamicStorage, element.Item6);
            TupleSchemaHelpers.WriteSeparator(fixedStorage, dynamicStorage);
            S7.Write(fixedStorage, dynamicStorage, element.Item7);
        }

        #endregion
    }

    static internal class TupleSchemaHelpers
    {
        internal static int FixedSeparatorSize = Encoding.EncodingSizeForSeparator;
        internal static int DynamicSeparatorSize = Encoding.EncodingSizeForSeparator;

        internal static void WriteSeparator(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage)
        {
            Encoding.WritePropertySeparator(fixedStorage);
            Encoding.WriteObjectSeparator(dynamicStorage);
        }

        internal static void SkipSeparators(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            Encoding.SkipPropertySeparator(fixedStorage);
            Encoding.SkipObjectSeparator(dynamicStorage);
        }
    }
}
