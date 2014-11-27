using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

namespace Persisted
{
    /// <summary>
    /// The interface that provides access to a storage such as a file system or a database.
    /// Stores containers or bytes. Each container is accessed by blocks of a fixed size.
    /// </summary>
    [ContractClass(typeof(IBlockStorageContract))]
    public interface IBlockStorage : IDisposable
    {
        #region Identifiers

        /// <summary>
        /// True iff a container with the given identifier exists in this storage
        /// </summary>
        bool Exists(Identifier containerId);

        /// <summary>
        /// Create a container associated with the given identifier
        /// </summary>
        ByteContainerHandle Create(Identifier containerId, int blockSize);

        /// <summary>
        /// Open the existing container associated with the given identifier
        /// </summary>
        ByteContainerHandle Open(Identifier containerId);

        #endregion

        #region Containers

        /// <summary>
        /// Signal the end of use of the container
        /// </summary>
        void Close(ByteContainerHandle container);

        /// <summary>
        /// Remove any storage associated with the container
        /// </summary>
        void Delete(ByteContainerHandle container);

        /// <summary>
        /// The size of the blocks in this storage. This should be constant
        /// </summary>
        int GetBlockSize(ByteContainerHandle container);

        /// <summary>
        /// Gets the current number of elements of the stored type currently contained in the container
        /// </summary>
        long GetBlockCount(ByteContainerHandle container);

        #endregion

        #region Read/Write

        /// <summary>
        /// Read a block of <see cref="BlockSize"/> bytes of type <typeparamref name="byte"/> from a container
        /// </summary>
        /// <param name="container">The container</param>
        /// <param name="position">The 0-based ID of the desired block </param>
        /// <param name="buffer">A buffer of the size specified for this container</param>
        Task ReadBlock(ByteContainerHandle container, long position, byte[] buffer);

        /// <summary>
        /// Write a block of <see cref="BlockSize"/> bytes of type <typeparamref name="byte"/> into a container.
        /// The write can be at an existing position between 0 and the block count, excluded, 
        /// in which case the existing content is overriden;
        /// OR, the write can be at position exactly block count, in which case a new block is created
        /// </summary>
        /// <param name="container">The container</param>
        /// <param name="position">The 0-based ID of the desired block </param>
        /// <param name="buffer">A buffer of the size specified for this container</param>
        Task WriteBlock(ByteContainerHandle container, long position, byte[] buffer);

        #endregion
    }


    /// <summary>
    /// A handle that references a container of bytes, used by the storage class
    /// </summary>
    public class ByteContainerHandle
    {
        #region Public Surface 

        /// <summary>
        /// The unique ID, or key, that designates the container.
        /// Should be standardized in the sense of <see cref="Persisted.Bytes.Identifiers"/>
        /// </summary>
        public Identifier Id { get; private set; }

        /// <summary>
        /// True if the container has been closed
        /// </summary>
        public bool Closed { get; internal set; }

        /// <summary>
        /// Mark the container as closed - 
        /// no operation on it will from now on be allowed.
        /// </summary>
        public void Close()
        {
            Closed = true;
        }

        #endregion

        #region Internals

        // Users should not be allowed to create handles
        // But an implementer who provides an IBlockStorage externally to the DLL 
        // should be able to create these handles, which are needed in the interface.
        // TODO:
        // Think this through when we consider a mechanism for external storage providers
        internal ByteContainerHandle(Identifier identifier)
        {
            Id = identifier;
            Closed = false;
        }

        #endregion
    }


    /// <summary>
    /// Codified contracts for IBlockStorage
    /// </summary>
    [ContractClassFor(typeof(IBlockStorage))]
    internal class IBlockStorageContract : IBlockStorage
    {
        #region Identifiers

        public bool Exists(Identifier containerId)
        {
            return default(bool);
        }

        public ByteContainerHandle Create(Identifier containerId, int blockSize)
        {
            Contract.Requires(blockSize > 0);
            Contract.Requires(!Exists(containerId));

            Contract.Ensures(!Contract.Result<ByteContainerHandle>().Closed);
            Contract.Ensures(GetBlockCount(Contract.Result<ByteContainerHandle>()) == 0);

            return default(ByteContainerHandle);
        }

        public ByteContainerHandle Open(Identifier containerId)
        {
            Contract.Requires(Exists(containerId));
            Contract.Ensures(!Contract.Result<ByteContainerHandle>().Closed);
            return default(ByteContainerHandle);
        }

        #endregion

        #region Containers

        public void Close(ByteContainerHandle container)
        {
            Contract.Requires(!container.Closed);
            Contract.Ensures(container.Closed);
        }

        public void Delete(ByteContainerHandle container)
        {
            Contract.Ensures(container.Closed);
            Contract.Ensures(!Exists(container.Id));
        }

        public int GetBlockSize(ByteContainerHandle container)
        {
            Contract.Requires(!container.Closed);
            return default(int);
        }

        public long GetBlockCount(ByteContainerHandle container)
        {
            Contract.Requires(!container.Closed);
            return default(long);
        }

        public void Dispose()
        {
        }

        #endregion

        #region Read / write

        public Task ReadBlock(ByteContainerHandle container, long position, byte[] buffer)
        {
            Contract.Requires(buffer.Length == GetBlockSize(container));
            Contract.Requires(0 <= position);
            Contract.Requires(position < GetBlockCount(container));
            return default(Task<byte[]>);
        }

        public Task WriteBlock(ByteContainerHandle container, long position, byte[] content)
        {
            Contract.Requires(0 <= position);
            Contract.Requires(position < GetBlockCount(container));
            Contract.Requires(content.Length == GetBlockSize(container));
            return default(Task);
        }

        #endregion

        #region Memory reuse 

        public void Release(byte[] buffer)
        {
        }

        #endregion
    }
}
