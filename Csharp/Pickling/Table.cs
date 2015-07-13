using System;
using System.Diagnostics.Contracts;

using Common;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    /// <summary>
    /// A contiguous, random access collection of elements of a certain Schema,
    /// transparently serialized into two ByteContainers.
    /// </summary>
    public class Table<T> : IDisposable
    {
        #region Provided fields

        /// <summary>
        /// The schema of the contained elements
        /// </summary>
        private readonly Schema<T> schema;

        /// <summary>
        /// A container that contains a number of contiguous elements
        /// </summary>
        private readonly ByteContainer data;

        /// <summary>
        /// The list of starting point and sizes in the data container
        /// </summary>
        private readonly ByteContainer index;

        private static readonly int IndexEntryEncodingSize = Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt;

        #endregion

        #region Cached data

        /// <summary>
        /// If the schema is fixed size we keep its size
        /// </summary>
        private Nullable<int> FixedSize;

        #endregion

        #region Construction and Disposal

        public Table(Schema<T> schema, ByteContainer fixedContainer, ByteContainer dynamicContainer)
        {
            this.index = fixedContainer;
            this.data = dynamicContainer;
            this.schema = schema;

            FixedSize = (schema.IsFixedSize)
                ? FixedSize = schema.GetDynamicSize(default(T))
                : null;


            ReadIndexBuffer = new ByteBuffer(IndexEntryEncodingSize);
            ReadIndexSegment = ReadIndexBuffer.GetReadView(0, IndexEntryEncodingSize);

            //          if (fixedContainer.Count % fixedEncodingSize != 0)
            //            throw new ArgumentException("Fixed storage should be of size multiple of the schema fixed size");
        }

        public void Dispose()
        {
            index.Dispose();
            data.Dispose();
        }

        #endregion

        #region Main methods

        public long Count
        {
            get
            {
                //                Contract.Invariant(index.Count % fixedEncodingSize == 0);
                //              return index.Count / fixedEncodingSize;

                return 0;
            }
        }


        private struct IndexEntry
        {
            /// <summary>
            /// Start of object encoding in data storage
            /// </summary>
            public long Start;

            /// <summary>
            /// Length of object encoding in data storage
            /// </summary>
            public int Length;
        }

        // Fields for use only in the ReadIndex method
        private readonly ByteBuffer ReadIndexBuffer = new ByteBuffer();
        private ByteSegmentReadView ReadIndexSegment;

        private IndexEntry ReadIndex(long position)
        {
            ReadIndexBuffer.Resize(Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt); // TODO: Once
            ReadIndexSegment = ReadIndexBuffer.GetReadView(0, IndexEntryEncodingSize); // TODO: REINITIALIZE INSTEAD 
            index.Read(ReadIndexSegment, position * IndexEntryEncodingSize);

            return new IndexEntry
            {
                Start = Encoding.ReadLong(ReadIndexSegment),
                Length = Encoding.ReadInt(ReadIndexSegment)
            };
        }

        // Fields for use only in the WriteIndex method
        private readonly ByteBuffer WriteIndexBuffer = new ByteBuffer();
        private ByteSegmentWriteView WriteIndexSegment;

        private void WriteIndex(long position, IndexEntry entry)
        {
            WriteIndexBuffer.Resize(Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt); // TODO: Once
            WriteIndexSegment = WriteIndexBuffer.GetWriteView(0, IndexEntryEncodingSize); // TODO: REINITIALIZE INSTEAD 
            Encoding.WriteLong(WriteIndexSegment, entry.Start);
            Encoding.WriteInt(WriteIndexSegment, entry.Length);
            index.Write(WriteIndexSegment, position * IndexEntryEncodingSize);
        }

        private readonly ByteBuffer ReadDataBuffer = new ByteBuffer();
        private ByteSegmentReadView ReadDataSegment;

        private T ReadData(long start, int size)
        {
            ReadDataBuffer.Resize(size);
            ReadDataSegment = ReadDataBuffer.GetReadView(0, size); // TODO: REINITIALIZE INSTEAD 
            data.Read(ReadDataSegment, start);
            return schema.Read(ReadDataSegment);
        }

        private readonly ByteBuffer WriteDataBuffer = new ByteBuffer();
        private ByteSegmentWriteView WriteDataSegment;

        private void WriteData(long position, T element, int size)
        {
            WriteDataBuffer.Resize(size);
            WriteDataSegment = WriteDataBuffer.GetWriteView(0, size); // TODO: REINITIALIZE INSTEAD 
            schema.Write(WriteDataSegment, element);
        }

        public T Read(long position)
        {
            if (FixedSize != null)
            {
                // optimizations
            }

            IndexEntry index = ReadIndex(position);
            return ReadData(index.Start, index.Length);
        }

        public void Write(long position, T element)
        {
            if (FixedSize != null)
            {
                // TODO: optimizations
            }
            
            long start = data.Count;
            int size = schema.GetDynamicSize(element);

            WriteIndex(position, new IndexEntry { Start = start, Length = size });
            WriteData(start, element, size);

            // TODO: Some assertions here!!

            // TODO: Keep track of fragmentation. This would require an index read before write...
        }

        #endregion
    }
}
