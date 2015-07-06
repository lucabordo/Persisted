using System;
using System.Diagnostics;

namespace Common
{
    /// <summary>
    /// A reusable array of bytes that can offers read and write views.
    /// </summary>
    public class ByteBuffer
    {
        private byte[] buffer;

        public ByteBuffer(int initializeSize = 128)
        {
            buffer = new byte[initializeSize];
        }

        /// <summary>
        /// Get the number of allocated bytes
        /// </summary>
        public int Capacity
        {
            get { return buffer.Length; }
        }

        /// <summary>
        /// Get read access to a contiguous segment of the array
        /// </summary>
        public ByteSegmentReadView GetReadView(int start, int endExclusive)
        {
            return new ByteSegmentReadView(buffer, start, endExclusive);
        }

        /// <summary>
        /// Get write access to a contiguous segment of the array
        /// </summary>
        public ByteSegmentWriteView GetWriteView(int start, int endExclusive)
        {
            return new ByteSegmentWriteView(buffer, start, endExclusive);
        }

        /// <summary>
        /// Make sure the capacity is at least some required amount
        /// </summary>
        public void Resize(int requiredSize, bool ignoreContent = false)
        {
            if (buffer.Length < requiredSize)
            {
                if (ignoreContent)
                    buffer = new byte[requiredSize];
                else
                    Array.Resize(ref buffer, requiredSize);
            }
        }
    }


    /// <summary>
    /// A read-only view on a segment of byte array.
    /// </summary>
    public class ByteSegmentReadView
    {
        private byte[] buffer_;
        private int start_;
        private int end_; // exclusive

        public ByteSegmentReadView(byte[] buffer, int start, int endExclusive)
        {
            Debug.Assert(0 <= start && start <= endExclusive && endExclusive <= buffer.Length);
            buffer_ = buffer;
            start_ = start;
            end_ = endExclusive;
        }

        public void Narrow(int startShift)
        {
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            start_ += startShift;
        }

        public int Count
        {
            get { return end_ - start_; }
        }

        public byte this[int index]
        {
            get
            {
                if (index < 0 || start_ + index >= end_)
                    throw new IndexOutOfRangeException();
                return buffer_[start_ + index];
            }
        }

        /// <summary>
        /// Optimized block copy 
        /// </summary>
        public void Read(int index, byte[] otherArray, int otherIndex, int length)
        {
            if (index < 0 || start_ + index + length > end_)
                throw new IndexOutOfRangeException();
            Array.Copy(buffer_, start_ + index, otherArray, otherIndex, length);
        }
    }


    /// <summary>
    /// A write-only view on a segment of byte array.
    /// </summary>
    public class ByteSegmentWriteView
    {
        private byte[] buffer_;
        private int start_;
        private int end_; // exclusive

        public ByteSegmentWriteView(byte[] buffer, int start, int endExclusive)
        {
            Debug.Assert(0 <= start && start <= endExclusive && endExclusive <= buffer.Length);
            buffer_ = buffer;
            start_ = start;
            end_ = endExclusive;
        }

        public void Narrow(int startShift)
        {
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            start_ += startShift;
        }

        public int Count
        {
            get { return end_ - start_; }
        }

        public byte this[int index]
        {
            set
            {
                if (index < 0 || start_ + index >= end_)
                    throw new IndexOutOfRangeException();
                buffer_[start_ + index] = value;
            }
        }

        /// <summary>
        /// Optimized block copy 
        /// </summary>
        public void Write(int index, byte[] otherArray, int otherIndex, int length)
        {
            if (index < 0 || start_ + index + length > end_)
                throw new IndexOutOfRangeException();
            Array.Copy(otherArray, otherIndex, buffer_, start_ + index, length);
        }
    }
}
