﻿using System;
using System.Diagnostics;

namespace Pickling
{
    // TODO
    // This is all Common, nothing specific to Pickling

    public class ByteBuffer
    {
        private byte[] buffer_;
        
        public ByteSegmentReadView GetReadView(int start, int endExclusive)
        {
            return new ByteSegmentReadView(buffer_, start, endExclusive);
        }

        public ByteSegmentWriteView GetWriteView(int start, int endExclusive)
        {
            return new ByteSegmentWriteView(buffer_, start, endExclusive);
        }

        /// <summary>
        /// Make sure the size is at least some required amount
        /// </summary>
        public void Resize(int requiredSize, bool ignoreContent = false)
        {
            if (buffer_.Length < requiredSize)
            {
                if (ignoreContent)
                    buffer_ = new byte[requiredSize];
                else
                    Array.Resize(ref buffer_, requiredSize);
            }
        }
    }


    /// <summary>
    /// A read-only view on a segment of byte array.
    /// </summary>
    public struct ByteSegmentReadView
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

        public ByteSegmentReadView Narrow(int startShift)
        {
            // TODO rather than all these chacks to 0 enforce uint? 
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            return new ByteSegmentReadView(buffer_, start_ + startShift, end_);
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
    public struct ByteSegmentWriteView
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

        public ByteSegmentWriteView Narrow(int startShift)
        {
            if (startShift < 0)
                throw new IndexOutOfRangeException();
            return new ByteSegmentWriteView(buffer_, start_ + startShift, end_);
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
