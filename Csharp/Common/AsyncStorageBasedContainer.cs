using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    class AsyncStorageBasedContainer : ByteContainer
    {
        #region Fields

        private readonly IAsyncStorage<byte> container_;
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
            public byte[] Elements;
        }

        #endregion

        public override long Count
        {
            get { throw new NotImplementedException(); }
        }

        public override void Dispose()
        {
            throw new NotImplementedException();
        }

        public override void Load(ByteBufferBlockWriter segment, long index)
        {
            throw new NotImplementedException();
        }

        public override void Store(ByteBufferBlockReader segment, long index)
        {
            throw new NotImplementedException();
        }
    }
}
