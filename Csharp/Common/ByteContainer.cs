﻿using System;

namespace Common
{
    /// <summary>
    /// A collection of bytes indexed by consecutive, long non-negative integers.
    /// Conceptually a resizable array of bytes, although many implementations may in fact use several of arrays of bytes. 
    /// </summary>
    /// <remarks>
    /// Unlike a List of bytes, the content is read by segments, for reasons of:
    /// efficiency (we don't want to cross interfaces for every byte read and write);
    /// and atomicity (a whole segment should read or written in one go).
    /// </remarks>
    public abstract class ByteContainer : IDisposable
    {
        /// <summary>
        /// Load into a segment the bytes contained at positions (index + segment.Count)
        /// </summary>
        public abstract void Load(ByteBufferBlockWriter segment, long index);
        
        /// <summary>
        /// Store the bytes from a segment into the storage at positions index to (index + segment.Count);
        /// The container may be extended if needed, but preserving contiguity.
        /// </summary>
        public abstract void Store(ByteBufferBlockReader segment, long index);

        /// <summary>
        /// Number of contiguous bytes effectively written.
        /// </summary>
        public abstract long Count { get; }

        /// <summary>
        /// Cleanup of any resource
        /// </summary>
        public abstract void Dispose();

        void IDisposable.Dispose()
        {
            this.Dispose();
        }
    }
}
