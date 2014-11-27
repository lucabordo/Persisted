using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

using Dir = System.IO.Directory;

namespace Persisted.Bytes
{
    /// <summary>
    /// A byte storage that uses the local file system
    /// </summary>
    /// <remarks>
    /// By convention the first 4 bytes of the storage are used to store the block size 
    /// </remarks>
    internal sealed class LocalFileStorage : IBlockStorage
    {
        #region Members and Properties

        /// <summary>
        /// The address of the Root directory where any content is persisted. 
        /// Standardized.
        /// </summary>
        /// <remarks>
        /// This should be self-contained and isolated:
        /// nobody else is supposed to write under this directory
        /// </remarks>
        private Identifier Root;

        /// <summary>
        /// The containers that are currently open;
        /// their key is their relative path, as found in their Identifier - not their full path
        /// </summary>
        private Dictionary<string, FileContainer> _openContainers = new Dictionary<string, FileContainer>();

        #endregion

        #region Construction and disposal

        internal LocalFileStorage(Identifier rootPath)
        {
            Root = rootPath;

            if (!Dir.Exists(Root))
                Dir.CreateDirectory(Root);
        }

        public static IBlockStorage Create(Identifier root)
        {
            return new LocalFileStorage(root);
        }

        public void Dispose()
        {
            foreach (var pair in _openContainers)
                pair.Value.Dispose();
        }

        #endregion

        #region Identifiers

        /// <summary>
        /// True iff a container with the given identifier exists in this storage
        /// </summary>
        public bool Exists(Identifier containerId)
        {
            string path = Path.Combine(Root, containerId);
            return File.Exists(path); 
        }

        /// <summary>
        /// Create a container associated with the given identifier
        /// </summary>
        public ByteContainerHandle Create(Identifier id, int blockSize)
        {
            string path = Path.Combine(Root, id);

            if (blockSize <= 0)
                throw new ArgumentException("block size should be positive");
            if (_openContainers.ContainsKey(id))
                throw new ArgumentException("File creation: file already exists");

            var directory = Path.GetDirectoryName(path);
            if (!Dir.Exists(directory))
                Dir.CreateDirectory(directory);

            var result = FileContainer.Create(id, path, blockSize);
            _openContainers.Add(id, result);
            return result;
        }

        /// <summary>
        /// Open the existing container associated with the given identifier.
        /// </summary>
        /// <remarks>
        /// Opening several times the same container is valid, 
        /// but returns a handle that is always reference-equal
        /// </remarks>
        public ByteContainerHandle Open(Identifier id)
        {
            string path = Path.Combine(Root, id);
            FileContainer result;

            if (!_openContainers.TryGetValue(id, out result))
            {
                result = FileContainer.Open(id, path);
                _openContainers.Add(id, result);
            }

            return result;
        }

        #endregion

        #region Containers

        /// <summary>
        /// Signal the end of use of the container
        /// </summary>
        public void Close(ByteContainerHandle container)
        {
            var handle = container as FileContainer;

            if (container.Closed)
                throw new Exception("Container is already closed");
            if (handle == null) 
                throw new Exception("Container handle is of the wrong type");

            handle.Dispose();
            bool removed = _openContainers.Remove(container.Id);
            Debug.Assert(removed);
        }

        /// <summary>
        /// Remove any storage associated with the container
        /// </summary>
        public void Delete(ByteContainerHandle container)
        {
            var handle = container as FileContainer;
            if (handle == null)
                throw new Exception("Container handle is of the wrong type");
            string path = handle.FullPath;
            Close(container);
            File.Delete(path);
        }

        /// <summary>
        /// The size of the blocks in this storage. This should be constant
        /// </summary>
        public int GetBlockSize(ByteContainerHandle container)
        {
            var handle = container as FileContainer;
            if (handle == null)
                throw new Exception("Container handle is of the wrong type");
            return handle.BlockSize;
        }

        /// <summary>
        /// Gets the current number of elements of the stored type currently contained in the container
        /// </summary>
        public long GetBlockCount(ByteContainerHandle container)
        {
            var handle = container as FileContainer;
            if (handle == null)
                throw new Exception("Container handle is of the wrong type");
            return handle.GetBlockCount();
        }

        #endregion

        #region Read/Write

        /// <summary>
        /// Read a block of <see cref="BlockSize"/> bytes of type <typeparamref name="byte"/> from a container
        /// </summary>
        /// <param name="container">The container</param>
        /// <param name="position">The 0-based ID of the desired block </param>
        /// <param name="buffer">A buffer of the size specified for this container</param>
        public Task ReadBlock(ByteContainerHandle container, long position, byte[] buffer)
        {
            var handle = container as FileContainer;
            if (handle == null)
                throw new Exception("Container handle is of the wrong type");
            return handle.ReadAsync(position, buffer);
        }

        /// <summary>
        /// Write a block of <see cref="BlockSize"/> bytes of type <typeparamref name="byte"/> into a container.
        /// The write can be at an existing position between 0 and the block count, excluded, 
        /// in which case the existing content is overriden;
        /// OR, the write can be at position exactly block count, in which case a new block is created
        /// </summary>
        /// <param name="container">The container</param>
        /// <param name="position">The 0-based ID of the desired block </param>
        /// <param name="buffer">A buffer of the size specified for this container</param>
        public Task WriteBlock(ByteContainerHandle container, long position, byte[] buffer)
        {
            var handle = container as FileContainer;
            if (handle == null)
                throw new Exception("Container handle is of the wrong type");
            return handle.WriteAsync(position, buffer);
        }

        #endregion

        #region Internal classes

        /// <summary>
        /// The container handles used by the local file storage
        /// </summary>
        internal class FileContainer : ByteContainerHandle
        {
            #region Fields and properties

            private const int _headerLength = 4;

            // We use a different stream for reading and writing. Not sure
            private FileStream _read;
            private FileStream _write;

            /// <summary>
            /// Get the block size, which is encoded at the beginning of the file
            /// </summary>
            public int BlockSize { get; private set; }

            /// <summary>
            /// The full path of the underlying file 
            /// </summary>
            public string FullPath { get; private set; }

            private Task _lastAction = null;

            #endregion

            #region Construction and disposal

            private FileContainer(Identifier id, string fullPath, FileStream readStream, FileStream writeStream, int blockSize)
                : base(id)
            {
                Debug.Assert(blockSize >= 8);
                FullPath = fullPath;
                _read = readStream;
                _write = writeStream;
                BlockSize = blockSize;
            }

            /// <summary>
            /// Open an existing container.
            /// This does a blocking read operation, to retrieve the block size encoded in the file header
            /// </summary>
            public static FileContainer Open(Identifier id, string fullPath)
            {
                Debug.Assert(_headerLength == 4);
                var read = GetReadStream(fullPath);
                var header = new byte[4];
                var countRead = read.ReadAsync(header, 0, 4).Result;
                if (countRead != 4)
                    throw new Exception("File header is missing");
                int blockSize = Utils.Converter32.FromBytes(header[0], header[1], header[2], header[3]);
                return new FileContainer(id, fullPath, read, null, blockSize);
            }

            /// <summary>
            /// Create a container. 
            /// This does a blocking write operation, to encode the block size in the file header
            /// </summary>
            public static FileContainer Create(Identifier id, string fullPath, int blockSize)
            {
                Debug.Assert(_headerLength == 4);
                var write = GetWriteStream(fullPath, FileMode.CreateNew);
                var header = new byte[4];
                Utils.Converter32.ToBytes(blockSize, out header[0], out header[1], out header[2], out header[3]);
                write.WriteAsync(header, 0, 4).Wait();
                write.Flush();
                return new FileContainer(id, fullPath, null, write, blockSize);
            }

            public void Dispose()
            {
                if (!Closed)
                {
                    Close();
                    if (_lastAction != null)
                        _lastAction.Wait();
                    if (_read != null)
                        _read.Dispose();
                    if (_write != null)
                        _write.Dispose();
                }
            }

            #endregion

            #region Main APIS

            public Task<int> ReadAsync(long position, byte[] buffer)
            {
                StartReadWrite(position, buffer);
                if (_read == null)
                    _read = GetReadStream(FullPath);

                _read.Seek(position * BlockSize + _headerLength, SeekOrigin.Begin);
                var action = _read.ReadAsync(buffer, 0, BlockSize);
                _lastAction = action;
                return action;
            }

            public Task WriteAsync(long position, byte[] buffer)
            {
                StartReadWrite(position, buffer);
                if (_write == null)
                    _write = GetWriteStream(FullPath, FileMode.Open);

                _write.Seek(position * BlockSize + _headerLength, SeekOrigin.Begin);
                var action = _write.WriteAsync(buffer, 0, BlockSize);
                _lastAction = action;
                return action;
            }

            public long GetBlockCount()
            {
                if (_read == null)
                    _read = GetReadStream(FullPath);
                long byteCount = _read.Length - _headerLength;
                Debug.Assert(byteCount % BlockSize == 0);
                return byteCount / BlockSize;
            }

            private void StartReadWrite(long position, byte[] buffer)
            {
                if (Closed)
                    throw new Exception("Container is closed and should not be used");
                if (buffer.Length != BlockSize)
                    throw new Exception("Buffer is not of the expected length");

                // The below would completely serialize the reads and writes.
                // This would give a strong consistency guarantee and limit the 
                // pressure we put on a single file at any time. 
                // Currently though it's the higher layer that provides
                // guarantees, at the bottom we should just do the async calls
                
                // if (_lastAction != null)
                //     _lastAction.Wait();
            }

            #endregion

            #region Private

            private static FileStream GetReadStream(string fullPath)
            {
                return new FileStream(
                    fullPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    4096,
                    FileOptions.Asynchronous);
            }

            private static FileStream GetWriteStream(string fullPath, FileMode mode)
            {
                return new FileStream(
                    fullPath,
                    mode,
                    FileAccess.Write,
                    FileShare.ReadWrite,
                    4096,
                    FileOptions.Asynchronous);
            }

            #endregion
        }

        #endregion
    }
}
