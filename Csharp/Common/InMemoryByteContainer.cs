using System;
using Common;

namespace Common
{
    /// <summary>
    /// A simple container whose storage is a simple, resizable in-memory array.
    /// This can be used as a reference implementation for testing. 
    /// </summary>
    public class InMemoryByteContainer : ByteContainer
    {
        private byte[] storage_;
        private long size_; // Due to Resize limitations this will be limited to int range

        public InMemoryByteContainer()
        {
            storage_ = new byte[8];
            size_ = 0;
        }

        /// <summary>
        /// Load into a segment the bytes contained at positions (index + segment.Count)
        /// </summary>
        public override void Load(ByteBufferBlockWriter segment, long index)
        {
            if (index + segment.Count > size_)
                throw new IndexOutOfRangeException();
            segment.Write(0, storage_, (int)index, segment.Count);
        }

        /// <summary>
        /// Store the bytes from a segment into the storage at positions index to (index + segment.Count);
        /// The container may be extended if needed, but preserving contiguity.
        /// </summary>
        public override void Store(ByteBufferBlockReader segment, long index)
        {
            if (index > size_)
                throw new IndexOutOfRangeException();
            while (storage_.Length < segment.Count + index)
                Array.Resize(ref storage_, storage_.Length * 2);

            segment.Read(0, storage_, (int)index, segment.Count);
            size_ = Math.Max(size_, segment.Count + index);
        }

        public override long Count
        {
            get { return size_; }
        }

        public override void Dispose()
        {

        }
    }
}
