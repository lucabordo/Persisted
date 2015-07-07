using System;
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
    /// A schema component for strings
    /// </summary>
    internal class StringSchema : Schema<string>
    {
        // TODO: schemas are stateful, therefore construct explicitly
        private char[] buffer_ = new char[128];

        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForOffset;
        }

        internal override int GetDynamicSize(string element)
        {
            return Encoding.EncodingSizeForString(element.Length);
        }

        internal override string Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            int length = Encoding.ReadInt(fixedStorage);
            return Encoding.ReadString(dynamicStorage, length, ref buffer_);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, string element)
        {
            Encoding.WriteInt(fixedStorage, element.Length);
            Encoding.WriteString(dynamicStorage, element);
        }
    }


    /// <summary>
    /// A schema component for generic arrays
    /// </summary>
    internal class InlineArray<T> : Schema<T[]>
    {
        readonly Schema<T> innerSchema;

        // TODO: adhere strictly to memory reuse policy :

        //ByteSegmentReadView cachedFixedWrite;
        //ByteSegmentReadView cachedDynamicWrite;
        //ByteSegmentWriteView cachedFixedRead;
        //ByteSegmentWriteView cachedDynamicRead;

        public InlineArray(Schema<T> schema)
        {
            innerSchema = schema;
        }

        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForInt;
        }

        internal override int GetDynamicSize(T[] element)
        {
            int size = innerSchema.GetFixedSize() * element.Length;

            foreach (var entry in element)
            {
                size += innerSchema.GetDynamicSize(entry);
            }

            return size;
        }

        internal override T[] Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            Contract.Assert(fixedStorage.Count == GetFixedSize());
            Contract.Assert(fixedStorage.Disjoint(dynamicStorage));

            int size = Encoding.ReadInt(fixedStorage);
            var result = new T[size];

            // We split the dynamic storage into two segments: 
            // One aimed at storing the fixed-size parts of the elements, one for the dynamic part
            int innerBlockSize = innerSchema.GetFixedSize() * size;
            var fixedPart = dynamicStorage.SubSegment(0, innerBlockSize);
            var dynamicPart = dynamicStorage.SubSegment(innerBlockSize, dynamicStorage.Count - innerBlockSize);

            for (int i = 0; i < size; ++i)
            {
                result[i] = innerSchema.Read(fixedPart, dynamicPart);
            }

            return result;
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, T[] element)
        {
            Contract.Assert(fixedStorage.Count == GetFixedSize());
            Contract.Assert(dynamicStorage.Count == GetDynamicSize(element));
            Contract.Assert(fixedStorage.Disjoint(dynamicStorage));
            
            // TODO: add marker? e.g. [size]
            Encoding.WriteInt(fixedStorage, element.Length);

            // We split the dynamic storage into two segments: 
            // One aimed at storing the fixed-size parts of the elements, one for the dynamic part
            int innerBlockSize = innerSchema.GetFixedSize() * element.Length;
            var fixedPart = dynamicStorage.SubSegment(0, innerBlockSize);
            var dynamicPart = dynamicStorage.SubSegment(innerBlockSize, dynamicStorage.Count - innerBlockSize);

            foreach (var entry in element)
            {
                innerSchema.Write(fixedPart, dynamicPart, entry);
            }
        }
    }
}
