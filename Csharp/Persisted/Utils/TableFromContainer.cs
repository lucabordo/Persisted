using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Persisted.Utils
{
    /// <summary>
    /// A Table that is actually based on a Container. 
    /// <para>
    ///   While a container provides a coarse-grained (block-based), asynchronous API,
    ///   this class wraps it to provide an fine-grained (array-like) synchronous API.
    ///   Elements can be accessed at random positions, but locality is required for 
    ///   efficiency, as with any cache.
    /// </para>
    /// <para>
    ///   To do this API adaptation, uses a cache of blocks 
    ///   that are read from and written back to the container asynchonously, i.e. 
    ///   reads happen ahead-of-time while writes are done as background operations. 
    /// </para>
    /// </summary>
    /// 
    /// <remarks>
    /// Improvements to consider:
    /// - consider maintaining stats at runtime on the number of unsuccessful 
    ///   pedictive loads to decide the probability of running predictions. 
    ///   (this could be decayed to capture changing recent behaviour)
    ///   the idea is: predictions should pay for sequential access like a foreach
    ///   but if an access pattern is random we should detect that predictive loads
    ///   consistently fail and decrease their frequency at runtime
    /// - consider bounded queues or background read and write operations of arbitrary
    ///   size - here it's just size 1. Note however that write queues of size > 1
    ///   might raise subtle issues when considering out of order block insertions in 
    ///   CreatePagesInOrder
    /// - specialize the cache for long (does the spurious GetHashCode() have a cost?)
    ///   and for specific sizes - if specific sizes make sense here
    /// - consider the idea of _CancelledTask: when a predictive load aborts, we may
    ///   cancel, or just ignore the Task (but keep it aside in order to bound the 
    ///   number of cancelled tasks that can be concurrently run on this container
    /// </remarks>
    internal class TableFromContainer<Element>: ITable<Element>, IDisposable
    {
        #region Fields

        private readonly IContainer<Element> _container;
        private readonly int _blockSize;
        private long _blockCount;

        private Cache<long, Page> _cache;

        /// <summary>
        /// An annotated block stored in the cache 
        /// </summary>
        private class Page
        {
            public bool IsModified;
            public long Id;
            public Element[] Elements;
        }

        #endregion

        #region Construction and Disposal

        public TableFromContainer(IContainer<Element> wrapperContainer, int cacheCapacity)
        {
            _container = wrapperContainer;
            _blockCount = wrapperContainer.BlockCount;
            _blockSize = wrapperContainer.BlockSize;

            var header = _container.Header;
            var converter = new Converter64(header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7]);
            ElementCount = converter.AsLong;

            _cache = new Cache<long, Page>(cacheCapacity, Load, Unload);
        }

        public void Dispose()
        {
            _cache.Clear();
            if (_backgroundWriteTask != null)
                _backgroundWriteTask.Wait();

            // this will update the count and save it 
            var converter = new Converter64();
            converter.AsLong = ElementCount;
            var header = _container.Header;
            header[0] = converter.Byte0;
            header[1] = converter.Byte1;
            header[2] = converter.Byte2;
            header[3] = converter.Byte3;
            header[4] = converter.Byte4;
            header[5] = converter.Byte5;
            header[6] = converter.Byte6;
            header[7] = converter.Byte7;
            _container.Dispose();
        }

        private static long ReadElementCount(byte[] header)
        {
            var converter = new Converter64(header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7]);
            return converter.AsLong;
        }

        #endregion

        #region Cache load and unload

        // Asynchronous read and write operations
        // Note that we could have more generally a queue of ongoing
        // background reads and writes. This queue would need to be bounded.
        // Here we essentially have a queue of size 1.

        // NOTE the following crucial invariants:
        // the IDs of the blocks in the _cache and the 2 below are disjoint;
        // The union of all these ideas is the set of blocks that require special care

        /// <summary>
        /// Called when an uncached block is required.
        /// </summary>
        private Page Load(long blockId)
        {
            Debug.Assert(0 <= blockId && blockId <= _blockCount);
            Element[] result;

            if (blockId == _blockCount)
            {
                // This is a new block;
                // saving it is legal, note however that the actual writes to disk are delayed
                // and might happen out of order
                _blockCount++;
                var defaultBuffer = new Element[_blockSize]; // will be recycled, but allocate fresh
                WaitBackgroundWrite();
                StartBackgroundWrite(blockId, defaultBuffer);
                result = AllocateOrReuseBlock();
                return new Page { Id = blockId, Elements = result, IsModified = true };
            }
            else
            {
                if (BlockIsBeingRead(blockId))
                {
                    // This branch happens in the good case where we are requested to return a block 
                    // that we have anticipated. We just wait for the started load to complete,
                    // prepare a result, and  prepare the next predictive load.
                    result = WaitBackgroundRead();
                }
                else
                {
                    // Any ongoing modification of the seeked block would 
                    // need to be completed before we can reload
                    if (BlockIsBeingWritten(blockId))
                    {
                        WaitBackgroundWrite();
                    }

                    // We wait for any other ongoing read. This is purely to avoid having unbounded 
                    // number of read operations simultaneously in the background
                    result = WaitBackgroundRead();
                    if (result != null)
                        _recycledPages.Push(result);

                    StartBackgroundRead(blockId);
                    result = WaitBackgroundRead();
                }

                // predictive load:
                // In sequential use, which is what we optimize for, we'll likely access the next block
                // Note the extra dictionary look-up here
                if (blockId + 1 < _blockCount &&
                    !BlockIsBeingRead(blockId + 1) && 
                    !BlockIsBeingWritten(blockId + 1) && 
                    !_cache.HasKey(blockId + 1))
                {
                    StartBackgroundRead(blockId + 1);
                }

                Debug.Assert(result.Length == _blockSize);
                return new Page { Id = blockId, Elements = result, IsModified = false };
            }
        }

        /// <summary>
        /// Called when a block is evicted from the cache
        /// </summary>
        private void Unload(long blockId, Page block)
        {
            Debug.Assert(block.Id == blockId);
            Debug.Assert(blockId < _container.BlockCount || block.IsModified); // new blocks must be marked modified

            if (block.IsModified)
            {
                // We wait for any pending save
                // This is purely to avoid having unbounded number of write operations simultaneously in the background
                WaitBackgroundWrite(); 
                StartBackgroundWrite(blockId, block.Elements);
            }
            else
            {
                _recycledPages.Push(block.Elements);
            }
        }

        // NOTE:
        // we could go further in reusing memory and saving allocations:
        // - reuse Page objects similarly to arrays
        // - Have ReadBlock return directly the result of _container.ReadBlock
        //   with the result stored in a field
        // - Same thing for WriteBlock whose continuation should be avoided and 
        //   could instead use a field
        // - if we do that well we might avoid any locking by doing all the 
        //   queue operations on the caller thread

        private long _backgroundReadId;
        private Task _backgroundReadTask;
        private Element[] _backgroundReadBuffer;

        private long _backgroundWriteId;
        private Task _backgroundWriteTask;
        private Element[] _backgroundWriteBuffer;

        private Stack<Element[]> _recycledPages = new Stack<Element[]>();

        private void StartBackgroundRead(long blockId)
        {
            Debug.Assert(_backgroundReadTask == null || _backgroundReadTask.IsCompleted);

            _backgroundReadId = blockId;
            _backgroundReadBuffer = AllocateOrReuseBlock();
            _backgroundReadTask = _container.ReadBlock(blockId, _backgroundReadBuffer);
        }

        private void StartBackgroundWrite(long blockId, Element[] buffer)
        {
            Debug.Assert(_backgroundWriteTask == null || _backgroundWriteTask.IsCompleted);
            Debug.Assert(buffer != null && buffer.Length == _blockSize);

            _backgroundWriteId = blockId;
            _backgroundWriteBuffer = buffer;

            // We need to be cautious with writes that extend the storage
            // They may happen out of order because the evictions of the blocks
            // created at the end of storage might happen out of order
            _backgroundWriteTask = _container.WriteBlock(blockId, buffer);
        }

        private Element[] WaitBackgroundRead()
        {
            if (_backgroundReadTask == null)
            {
                return null;
            }
            else
            {
                _backgroundReadTask.Wait();
                _backgroundReadTask = null;
                return _backgroundReadBuffer;
            }
        }

        private void WaitBackgroundWrite()
        {
            if (_backgroundWriteTask != null)
            {
                _backgroundWriteTask.Wait();
                _backgroundWriteTask = null;
                _recycledPages.Push(_backgroundWriteBuffer);
            }
        }

        private bool BlockIsBeingRead(long blockId)
        {
            return (_backgroundReadTask != null && blockId == _backgroundReadId);
        }

        private bool BlockIsBeingWritten(long blockId)
        {
            return (_backgroundWriteTask != null && blockId == _backgroundWriteId);
        }

        private Element[] AllocateOrReuseBlock()
        {
            if (_recycledPages.Count > 0)
            {
                var result =  _recycledPages.Pop();
                //Array.Clear(result, 0, _blockSize);
                Debug.Assert(result != null && result.Length == _blockSize);
                return result;
            }
            else
            {
                return new Element[_blockSize];
            }
        }

        #endregion

        #region Table interface - Element Count

        /// <summary>
        /// Number of elements currently stored
        /// </summary>
        public long ElementCount
        {
            get;
            private set;
        }

        #endregion

        #region Table interface - synchronous access to individual elements

        /// <summary>
        /// The last accessed block, 
        /// </summary>
        private Page _lastAccessedBlock = new Page { Id = -1, IsModified = false, Elements = null };

        /// <summary>
        /// Access an individual element
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Element Read(long position)
        {
            if (position < 0 || position >= ElementCount)
            {
                throw new System.IndexOutOfRangeException();
            }

            long blockId = position / _blockSize;
            int positionInBlock = (int)(position % _blockSize);

            Page block = GetBlock(blockId);
            return block.Elements[positionInBlock];
        }

        /// <summary>
        /// Write an individual element.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(long position, Element newValue)
        {
            if (position < 0 || position > ElementCount)
            {
                throw new System.IndexOutOfRangeException();
            }
            if (position == ElementCount)
            {
                ElementCount++;
            }

            long blockId = position / _blockSize;
            int positionInBlock = (int)(position % _blockSize);

            Page block = GetBlock(blockId);
            block.IsModified = true;
            block.Elements[positionInBlock] = newValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Page GetBlock(long blockId)
        {
            if (_lastAccessedBlock.Id == blockId)
            {
                // This is the big shortcut
                // We should fall into this case as often as possible
                // It should be as efficient and inlinable as possible (bypass interface call)
                return _lastAccessedBlock;
            }
            else
            {
                // Backup plan
                // We need to look into caches, possibly wait for pending asynchronous communications or new ones
                // Should be called as rarely as possible and does lots of stuff so should in fact preferably not be inlined
                return GetBlockFromCache(blockId);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private Page GetBlockFromCache(long blockId)
        {
            // Note that blockId == _blockCount is allowed - block addition
            Debug.Assert(0 <= blockId && blockId <= _blockCount);

            _lastAccessedBlock = _cache[blockId];
            return _lastAccessedBlock;
        }

        #endregion
    }
}
