using Persisted;
using System;

namespace Persisted.Typed
{
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
            S1 = s1; S2 = s2;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
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
            S1 = s1; S2 = s2; S3 = s3;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + S3.GetSize(encoding) + 2 * encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2, T3> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i3 = S3.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2, i3);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2, T3> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
            encoding.WritePropertySeparator(target, ref position);
            S3.Write(image, encoding, ref position, element.Item3);
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
            S1 = s1; S2 = s2; S3 = s3; S4 = s4;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + S3.GetSize(encoding) + S4.GetSize(encoding) + 3 * encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2, T3, T4> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i3 = S3.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i4 = S4.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2, i3, i4);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2, T3, T4> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
            encoding.WritePropertySeparator(target, ref position);
            S3.Write(image, encoding, ref position, element.Item3);
            encoding.WritePropertySeparator(target, ref position);
            S4.Write(image, encoding, ref position, element.Item4);
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
            S1 = s1; S2 = s2; S3 = s3; S4 = s4; S5 = s5;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + S3.GetSize(encoding) + S4.GetSize(encoding) +
                S5.GetSize(encoding) + 4 * encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2, T3, T4, T5> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i3 = S3.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i4 = S4.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i5 = S5.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2, i3, i4, i5);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2, T3, T4, T5> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
            encoding.WritePropertySeparator(target, ref position);
            S3.Write(image, encoding, ref position, element.Item3);
            encoding.WritePropertySeparator(target, ref position);
            S4.Write(image, encoding, ref position, element.Item4);
            encoding.WritePropertySeparator(target, ref position);
            S5.Write(image, encoding, ref position, element.Item5);
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
            S1 = s1; S2 = s2; S3 = s3; S4 = s4; S5 = s5; S6 = s6;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + S3.GetSize(encoding) + S4.GetSize(encoding) +
                S5.GetSize(encoding) + S6.GetSize(encoding) + 5 * encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i3 = S3.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i4 = S4.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i5 = S5.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i6 = S6.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2, i3, i4, i5, i6);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2, T3, T4, T5, T6> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
            encoding.WritePropertySeparator(target, ref position);
            S3.Write(image, encoding, ref position, element.Item3);
            encoding.WritePropertySeparator(target, ref position);
            S4.Write(image, encoding, ref position, element.Item4);
            encoding.WritePropertySeparator(target, ref position);
            S5.Write(image, encoding, ref position, element.Item5);
            encoding.WritePropertySeparator(target, ref position);
            S6.Write(image, encoding, ref position, element.Item6);
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
            S1 = s1; S2 = s2; S3 = s3; S4 = s4; S5 = s5; S6 = s6; S7 = s7;
        }

        #endregion

        #region Methods

        internal override int GetSize(Encoding encoding)
        {
            return S1.GetSize(encoding) + S2.GetSize(encoding) + S3.GetSize(encoding) + S4.GetSize(encoding) +
                S5.GetSize(encoding) + S6.GetSize(encoding) + S7.GetSize(encoding) + 6 * encoding.EncodingSizeForSeparator;
        }

        internal override Tuple<T1, T2, T3, T4, T5, T6, T7> Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var source = image.PrimaryContainer;
            var i1 = S1.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i2 = S2.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i3 = S3.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i4 = S4.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i5 = S5.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i6 = S6.Read(image, encoding, ref position);
            encoding.SkipPropertySeparator(source, ref position);
            var i7 = S7.Read(image, encoding, ref position);
            return Tuple.Create(i1, i2, i3, i4, i5, i6, i7);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, Tuple<T1, T2, T3, T4, T5, T6, T7> element)
        {
            var target = image.PrimaryContainer;
            S1.Write(image, encoding, ref position, element.Item1);
            encoding.WritePropertySeparator(target, ref position);
            S2.Write(image, encoding, ref position, element.Item2);
            encoding.WritePropertySeparator(target, ref position);
            S3.Write(image, encoding, ref position, element.Item3);
            encoding.WritePropertySeparator(target, ref position);
            S4.Write(image, encoding, ref position, element.Item4);
            encoding.WritePropertySeparator(target, ref position);
            S5.Write(image, encoding, ref position, element.Item5);
            encoding.WritePropertySeparator(target, ref position);
            S6.Write(image, encoding, ref position, element.Item6);
            encoding.WritePropertySeparator(target, ref position);
            S7.Write(image, encoding, ref position, element.Item7);
        }

        #endregion
    }
}
