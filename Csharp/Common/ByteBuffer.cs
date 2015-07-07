﻿using System;
using System.Diagnostics.Contracts;

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
    /// A view on a segment of byte array
    /// </summary>
    /// <remarks>
    /// These are conceptually structs, but are modified by callers (stream behaviour)
    /// and should then be passed by ref. Instead we use references. 
    /// We simply avoid reallocating. 
    /// </remarks>
    public class ByteSegment
    {
        protected byte[] buffer_;
        protected int start_;
        protected int end_; // exclusive

        internal ByteSegment(byte[] buffer, int start, int endExclusive)
        {
            Contract.Assert(0 <= start && start <= endExclusive && endExclusive <= buffer.Length);
            buffer_ = buffer;
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

        internal void SetAsSubSegmentFrom(ByteSegmentReadView other, int startShift, int length)
        {
            Contract.Requires(startShift >= 0 && start_ + startShift + length <= end_);
            buffer_ = other.buffer_;
            start_ = other.start_ + startShift;
            end_ = start_ + length;
        }

        public ByteSegmentReadView SubSegment(int startShift, int length)
        {
            if (startShift < 0 || start_ + startShift + length >= end_)
                throw new IndexOutOfRangeException();

            int newStart = start_ + startShift;
            return new ByteSegmentReadView(buffer_, newStart, newStart + length);
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

        internal void SetAsSubSegmentFrom(ByteSegmentWriteView other, int startShift, int length)
        {
            Contract.Requires(startShift >= 0 && start_ + startShift + length <= end_);
            buffer_ = other.buffer_;
            start_ = other.start_ + startShift;
            end_ = start_ + length;
        }

        public ByteSegmentWriteView SubSegment(int startShift, int length)
        {
            if (startShift < 0 || start_ + startShift + length >= end_)
                throw new IndexOutOfRangeException();

            int newStart = start_ + startShift;
            return new ByteSegmentWriteView(buffer_, newStart, newStart + length);
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
            if (index < 0 || start_ + index + length > end_)
                throw new IndexOutOfRangeException();
            Array.Copy(otherArray, otherIndex, buffer_, start_ + index, length);
        }
    }
}
