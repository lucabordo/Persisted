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
            // Provided fields
            this.index = fixedContainer;
            this.data = dynamicContainer;
            this.schema = schema;

            // Cached data
            FixedSize = (schema.IsFixedSize)
                ? FixedSize = schema.GetDynamicSize(default(T))
                : null;


            // Buffer allocations
            ReadIndexBuffer = new ByteBuffer(IndexEntryEncodingSize);
            ReadIndexSegment = ReadIndexBuffer.GetReadView();

            WriteIndexBuffer = new ByteBuffer(IndexEntryEncodingSize);
            WriteIndexSegment = WriteIndexBuffer.GetWriteView();

            ReadDataBuffer = new ByteBuffer();
            ReadDataSegment = ReadDataBuffer.GetReadView();

            WriteDataBuffer = new ByteBuffer();
            WriteDataSegment = WriteDataBuffer.GetWriteView();

            // Checks
            //          if (fixedContainer.Count % fixedEncodingSize != 0)
            //            throw new ArgumentException("Fixed storage should be of size multiple of the schema fixed size");
        }

        public void Dispose()
        {
            index.Dispose();
            data.Dispose();
        }

        #endregion

        #region Access to Index storage

        /// <summary>
        /// Size required to encode a object of type <see cref="IndexEntry"/>
        /// </summary>
        private static readonly int IndexEntryEncodingSize = Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt;

        /// <summary>
        /// Entry stored in the Index,
        /// represents the exact byte positions where an object is encoded in the data storage.
        /// </summary>
        /// <remarks>
        /// This allows in theory constant-time lookup of each stored object's representation
        /// </remarks>
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
        private readonly ByteBuffer ReadIndexBuffer;
        private ByteSegmentReadView ReadIndexSegment;

        private IndexEntry ReadIndex(long position)
        {
            // Bounds checking

            // Reset the buffers
            Contract.Requires(ReadIndexBuffer.Capacity == IndexEntryEncodingSize);
            ReadIndexBuffer.ResetView(ReadIndexSegment, 0, IndexEntryEncodingSize);

            // Load segment from storage
            index.Read(ReadIndexSegment, position * IndexEntryEncodingSize);

            // Decode segment
            long start = Encoding.ReadLong(ReadIndexSegment);
            int length = Encoding.ReadInt(ReadIndexSegment);
            return new IndexEntry { Start = start, Length = length };
        }

        // Fields for use only in the WriteIndex method
        private readonly ByteBuffer WriteIndexBuffer;
        private ByteSegmentWriteView WriteIndexSegment;

        private void WriteIndex(long position, IndexEntry entry)
        {
            // Bounds checking

            // Reset the buffers
            Contract.Requires(WriteIndexBuffer.Capacity == IndexEntryEncodingSize);
            WriteIndexBuffer.ResetView(WriteDataSegment, 0, IndexEntryEncodingSize);

            // Encode segment
            Encoding.WriteLong(WriteIndexSegment, entry.Start);
            Encoding.WriteInt(WriteIndexSegment, entry.Length);

            // Store the segment into storage
            index.Write(WriteIndexSegment, position * IndexEntryEncodingSize);
        }

        #endregion

        #region Access to Data storage

        // Fields for use only in the ReadData method
        private readonly ByteBuffer ReadDataBuffer;
        private ByteSegmentReadView ReadDataSegment;

        private T ReadData(long start, int size)
        {
            // Reset the buffers
            ReadDataBuffer.Resize(size);
            ReadDataBuffer.ResetView(ReadDataSegment, 0, size);

            // Load segment from storage
            data.Read(ReadDataSegment, start);

            // Decode segment
            return schema.Read(ReadDataSegment);
        }

        // Fields for use only in the WriteData method
        private readonly ByteBuffer WriteDataBuffer;
        private ByteSegmentWriteView WriteDataSegment;

        private void WriteData(long start, T element, int size)
        {
            // Reset the buffers
            WriteDataBuffer.Resize(size);
            WriteDataBuffer.ResetView(WriteDataSegment, 0, size);

            // Encode segment
            schema.Write(WriteDataSegment, element);

            // Store the segment into storage
            data.Write(WriteDataSegment, start);
        }

        #endregion

        #region Main methods

        /// <summary>
        /// Get the number of stored elements
        /// </summary>
        public long Count
        {
            get
            {
                Contract.Invariant(index.Count % IndexEntryEncodingSize == 0);
                return index.Count / IndexEntryEncodingSize;
            }
        }

        /// <summary>
        /// Read the <paramref name="position"/>-th stored element
        /// </summary>
        public T Read(long position)
        {
            if (FixedSize.HasValue)
            {
                // When the encoding has fixed size we never write the index, and have no fragmentation 
                return ReadData(position * FixedSize.Value, FixedSize.Value);
            }
            else
            {
                IndexEntry index = ReadIndex(position);
                return ReadData(index.Start, index.Length);
            }
        }

        /// <summary>
        /// Overwrite the data stored at the given position, or
        /// if the <code>position == Count</code>, create a new position
        /// </summary>
        public void Write(long position, T element)
        {
            if (FixedSize.HasValue)
            {
                // When the encoding has fixed size we never write the index, and have no fragmentation 
                WriteData(position * FixedSize.Value, element, FixedSize.Value);
            }
            else
            {
                long start = data.Count;
                int size = schema.GetDynamicSize(element);

                WriteIndex(position, new IndexEntry { Start = start, Length = size });
                WriteData(start, element, size);
            }

            // TODO: Some assertions here!!

            // TODO: Keep track of fragmentation. This would require an index read before write...
        }

        #endregion
    }
}
