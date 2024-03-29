﻿using System;
using System.Diagnostics.Contracts;

namespace Common
{
    /// <summary>
    /// A reusable array of bytes that can offer read and write views.
    /// </summary>
    public class ByteBuffer
    {
        #region State

        /// <summary>
        /// An array reused for frequent communications between schemas/encoders and storage
        /// </summary>
        private byte[] buffer;

        #endregion

        #region Initialization and resize

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

        #endregion

        #region Cursors

        /// <summary>
        /// Get a cursor allowing to read from a given position in the buffer
        /// </summary>
        public ByteBufferReadCursor GetReadCursor(int start = 0, int endExclusive = int.MinValue)
        {
            if (endExclusive < 0)
                endExclusive = buffer.Length;
            return new ByteBufferReadCursor(buffer, start, endExclusive);
        }

        /// <summary>
        /// Get a cursor allowing to write into the buffer from a given position
        /// </summary>
        public ByteBufferWriteCursor GetWriteCursor(int start = 0, int endExclusive = int.MinValue)
        {
            if (endExclusive < 0)
                endExclusive = buffer.Length;
            return new ByteBufferWriteCursor(buffer, start, endExclusive);
        }

        /// <summary>
        /// Reinitialize a read or write cursor to the start of the buffer
        /// </summary>
        public void Reset(ByteBufferCursor segment, int endExclusive)
        {
            // Buffer needs to be passed because of possible Resizes - 
            // otherwise ByteBufferCursor would need a reference to the object, not its internal array
            Contract.Requires(ReferenceEquals(segment.buffer, this.buffer));
            segment.Reset(buffer, 0, endExclusive);
        }

        #endregion

        #region Access by block

        /// <summary>
        /// Get a view that allows to read bytes from the buffer, by blocks
        /// </summary>
        public ByteBufferBlockReader GetBlockReader(int length = int.MinValue)
        {
            if (length < 0)
                length = buffer.Length;
            return new ByteBufferBlockReader(buffer, length);
        }

        /// <summary>
        /// Get a view that allows to write bytes into the buffer, by blocks
        /// </summary>
        public ByteBufferBlockWriter GetBlockWriter(int length = int.MinValue)
        {
            if (length < 0)
                length = buffer.Length;
            return new ByteBufferBlockWriter(buffer, length);
        }

        /// <summary>
        /// Reinitialize the bounds of a read or write block view
        /// </summary>
        public void Reset(ByteBufferBlockView segment, int length)
        {
            // Buffer needs to be passed because of possible Resizes - 
            // otherwise ByteBufferBlockView would need a reference to the object, not its internal array
            Contract.Requires(ReferenceEquals(segment.buffer, this.buffer));
            segment.Reset(buffer, length);
        }

        #endregion
    }

    #region BlockViews - reading and writing by blocks

    /// <summary>
    /// A view on a byte array
    /// </summary>
    /// <summary>
    /// Meant primarily to be used in the <see cref="ByteContainer"/> class,
    /// when storing bytes into, or loading bytes into a container.
    /// </summary>
    public class ByteBufferBlockView
    {
        internal byte[] buffer;
        internal int length;

        internal ByteBufferBlockView(byte[] buffer, int length)
        {
            Reset(buffer, length);
        }

        /// <summary>
        /// Re-initialize the length of the segment
        /// </summary>
        /// <remarks>
        /// Only code that owns the ByteBuffer will be able call this operation, through ByteBuffer's Reset method.
        /// </remarks>
        internal void Reset(byte[] buffer, int length)
        {
            this.buffer = buffer;
            this.length = length;
        }

        /// <summary>
        /// Number of bytes in the segment
        /// </summary>
        public int Count
        {
            get { return length; }
        }
    }

    /// <summary>
    /// A read-only view on a byte buffer.
    /// </summary>
    /// <summary>
    /// Meant primarily to be used in the <see cref="ByteContainer"/> class,
    /// when loading bytes from storage.
    /// </summary>
    public class ByteBufferBlockReader : ByteBufferBlockView
    {
        internal ByteBufferBlockReader(byte[] buffer, int length)
            : base(buffer, length)
        {
        }

        /// <summary>
        /// Block copy of bytes from the buffer
        /// </summary>
        public void Read(int bufferStart, byte[] destinationArray, int destinationStart, int countToCopy)
        {
            if (bufferStart < 0 || bufferStart + countToCopy > length)
                throw new IndexOutOfRangeException();
            Array.Copy(buffer, bufferStart, destinationArray, destinationStart, countToCopy);
        }
    }


    /// <summary>
    /// A write-only view on a byte buffer.
    /// </summary>
    /// <summary>
    /// Meant primarily to be used in the <see cref="ByteContainer"/> class,
    /// when storing bytes into a container.
    /// </summary>
    public class ByteBufferBlockWriter : ByteBufferBlockView
    {
        internal ByteBufferBlockWriter(byte[] buffer, int length)
            : base(buffer, length)
        {
        }

        /// <summary>
        /// Block copy of bytes to the buffer
        /// </summary>
        public void Write(int bufferStart, byte[] sourceArray, int sourceIndex, int countToCopy)
        {
            if (bufferStart < 0 || bufferStart + countToCopy > length)
                throw new IndexOutOfRangeException();
            Array.Copy(sourceArray, sourceIndex, buffer, bufferStart, countToCopy);
        }
    }

    #endregion

    #region Cursors - reading and writing byte by byte

    /// <summary>
    /// A view on a segment of byte array
    /// </summary>
    /// <remarks>
    /// These are conceptually structs, but are modified by callers (stream behaviour)
    /// and should then be passed by ref. Instead we use references. We simply avoid reallocating. 
    /// </remarks>
    public class ByteBufferCursor
    {
        internal byte[] buffer;
        internal int start;
        internal int end; // exclusive

        internal ByteBufferCursor(byte[] buffer, int start, int endExclusive)
        {
            Reset(buffer, start, endExclusive);
        }

        /// <summary>
        /// Re-initialize the bounds of the segment
        /// </summary>
        /// <remarks>
        /// Can be called only through ByteBuffer; 
        /// This makes sure that only code that owns the ByteBuffer can do this operation,
        /// which is conceptually internal.
        /// </remarks>
        internal void Reset(byte[] buffer, int start, int endExclusive)
        {
            Contract.Assert(0 <= start && start <= endExclusive && endExclusive <= buffer.Length);
            this.buffer = buffer;
            this.start = start;
            end = endExclusive;
        }

        /// <summary>
        /// Number of bytes in the segment
        /// </summary>
        public int Count
        {
            get { return end - start; }
        }
    }

    /// <summary>
    /// A read-only view on a segment of byte array.
    /// </summary>
    public class ByteBufferReadCursor : ByteBufferCursor
    {
        internal ByteBufferReadCursor(byte[] buffer, int start, int endExclusive) :
            base(buffer, start, endExclusive)
        {
        }

        public void MoveForward(int startShift)
        {
            Contract.Requires(startShift >= 0);
            start += startShift;
        }

        public byte NextChar
        {
            get { return buffer[start++]; }
        }

        public byte this[int index]
        {
            get
            {
                Contract.Requires(index >= 0 && start + index < end);
                return buffer[start + index];
            }
        }
    }

    /// <summary>
    /// A write-only view on a segment of byte array.
    /// </summary>
    public class ByteBufferWriteCursor : ByteBufferCursor
    {
        internal ByteBufferWriteCursor(byte[] buffer, int start, int endExclusive)
            : base(buffer, start, endExclusive)
        {
        }

        public void MoveForward(int startShift)
        {
            Contract.Requires(startShift >= 0);
            start += startShift;
        }

        public byte NextChar
        {
            set { buffer[start++] = value; }
        }

        public byte this[int index]
        {
            set
            {
                Contract.Requires(index >= 0 && start + index < end);
                buffer[start + index] = value;
            }
        }
    }

    #endregion
}
