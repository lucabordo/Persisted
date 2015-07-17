using System;
using System.Diagnostics.Contracts;

namespace Common
{
    /// <summary>
    /// A reusable array of bytes that can offer read and write views.
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
        public ByteSegmentReadView GetReadView(int start = 0, int endExclusive = int.MinValue)
        {
            if (endExclusive == int.MinValue)
                endExclusive = buffer.Length;
            return new ByteSegmentReadView(buffer, start, endExclusive);
        }

        /// <summary>
        /// Get write access to a contiguous segment of the array
        /// </summary>
        public ByteSegmentWriteView GetWriteView(int start = 0, int endExclusive = int.MinValue)
        {
            if (endExclusive == int.MinValue)
                endExclusive = buffer.Length;
            return new ByteSegmentWriteView(buffer, start, endExclusive);
        }

        /// <summary>
        /// Reinitialize the bounds of a read or write view
        /// </summary>
        public void ResetView(ByteSegment segment, int start, int endExclusive)
        {
            Contract.Requires(ReferenceEquals(segment.buffer_, this.buffer));
            segment.Reset(start, endExclusive);
        }

        /// <summary>
        /// Make sure the capacity is at least some required amount
        /// </summary>
        public void Resize(int requiredSize, bool ignoreContent = false)
        {
            while (buffer.Length < requiredSize)
            {
                if (ignoreContent)
                    buffer = new byte[buffer.Length * 2];
                else
                    Array.Resize(ref buffer, buffer.Length * 2);
            }
        }
    }


    /// <summary>
    /// A view on a segment of byte array
    /// </summary>
    /// <remarks>
    /// These are conceptually structs, but are modified by callers (stream behaviour)
    /// and should then be passed by ref. Instead we use references. We simply avoid reallocating. 
    /// </remarks>
    public class ByteSegment
    {
        internal byte[] buffer_;
        internal int start_;
        internal int end_; // exclusive

        internal ByteSegment(byte[] buffer, int start, int endExclusive)
        {
            Contract.Assert(0 <= start && start <= endExclusive && endExclusive <= buffer.Length);
            buffer_ = buffer;
            start_ = start;
            end_ = endExclusive;
        }

        /// <summary>
        /// Re-initialize the bounds of the segment
        /// </summary>
        /// <remarks>
        /// Can be called only through ByteBuffer; 
        /// This makes sure that only code that owns the ByteBuffer can do this operation,
        /// which is conceptually internal.
        /// </remarks>
        internal void Reset(int start, int endExclusive)
        {
            start_ = start;
            end_ = endExclusive;
        }

        /// <summary>
        /// True if two segments cover separate memory areas
        /// </summary>
        public bool Disjoint(ByteSegment other)
        {
            return !ReferenceEquals(buffer_, other.buffer_)
                || start_ >= other.end_
                || end_ <= other.start_;
        }

        /// <summary>
        /// Number of bytes in the segment
        /// </summary>
        public int Count
        {
            get { return end_ - start_; }
        }
    }


    /// <summary>
    /// A read-only view on a segment of byte array.
    /// </summary>
    public class ByteSegmentReadView : ByteSegment
    {
        internal ByteSegmentReadView(byte[] buffer, int start, int endExclusive):
            base(buffer, start, endExclusive)
        {
        }

        public void MoveForward(int startShift)
        {
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            start_ += startShift;
        }

        public byte NextChar
        {
            get { return buffer_[start_++]; }
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
            Contract.Requires(start_ == 0);
            if (index < 0 || start_ + index + length > end_)
                throw new IndexOutOfRangeException();
            Array.Copy(buffer_, start_ + index, otherArray, otherIndex, length);
        }
    }


    /// <summary>
    /// A write-only view on a segment of byte array.
    /// </summary>
    public class ByteSegmentWriteView : ByteSegment
    {
        internal ByteSegmentWriteView(byte[] buffer, int start, int endExclusive)
            : base(buffer, start, endExclusive)
        {
        }

        public void MoveForward(int startShift)
        {
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            start_ += startShift;
        }

        public byte NextChar
        {
            set { buffer_[start_++] = value; }
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
            Contract.Requires(start_ == 0);
            if (index < 0 || start_ + index + length > end_)
                throw new IndexOutOfRangeException();
            Array.Copy(otherArray, otherIndex, buffer_, start_ + index, length);
        }
    }
}
