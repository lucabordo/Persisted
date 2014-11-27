using System.Threading.Tasks;
using Persisted.Utils;

namespace Persisted.Bytes
{
    /// <summary>
    /// Root class for containers, i.e.
    /// collection of blocks of elements of a certain types, persisted in a storage.
    /// </summary>
    internal class ByteContainer : IContainer<byte>
    {
        #region Fields

        protected readonly IBlockStorage _storage;
        protected readonly ByteContainerHandle _container;
        protected readonly int _blockSize;

        #endregion

        #region Construction and Disposal

        public ByteContainer(IBlockStorage storage, ByteContainerHandle container)
        {
            _storage = storage;
            _container = container;

            _blockSize = _storage.GetBlockSize(_container);
            Header = new byte[_blockSize];

            // Block 0 is the header; 
            // We make sure that even for a new container we have this block
            if (_storage.GetBlockCount(_container) == 0)
            {
                _storage.WriteBlock(_container, 0, Header).Wait();
            }
            else
            {
                _storage.ReadBlock(_container, 0, Header).Wait();
            }
        }

        public void Dispose()
        {
            _storage.WriteBlock(_container, 0, Header).Wait();
        }

        #endregion

        #region IContainer interface

        public int BlockSize
        {
            get { return _blockSize; }
        }

        public long BlockCount
        {
            get { return _storage.GetBlockCount(_container); }
        }

        public Task ReadBlock(long position, byte[] buffer)
        {
            return _storage.ReadBlock(_container, position + 1, buffer);
        }

        public Task WriteBlock(long position, byte[] content)
        {
            return _storage.WriteBlock(_container, position + 1, content);
        }

        public byte[] Header 
        { 
            get; private set; 
        }

        #endregion
    }


    /// <summary>
    /// Some extensions that make it easier to find 
    /// ByteContainers when using a storage
    /// </summary>
    internal static class BlockStorageExtensions
    {
        /// <summary>
        /// Get a view on an existing container 
        /// </summary>
        public static ByteContainer GetContainer(this IBlockStorage storage, Identifier containerId)
        {
            var handle = storage.Open(containerId);
            return new ByteContainer(storage, handle);
        }
    }
}
