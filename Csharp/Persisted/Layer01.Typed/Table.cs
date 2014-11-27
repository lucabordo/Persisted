using System;
using System.Diagnostics;
using Persisted.Utils;

namespace Persisted.Typed
{
    /// <summary>
    /// A Table that contains fixed-size entries 
    /// </summary>
    /// <remarks>
    /// TODO: evaluate the performance and need for extra caching
    /// Right now in this version the TableFromContainer objects stored in the TableByteRepresentation 
    /// use a cache; but here every read or write does some parsing
    /// </remarks>
    internal class Table<T> : ITable<T>, IDisposable
    {
        #region Fields 

        private readonly TableByteRepresentation _container;
        private readonly Schema<T> _schema;
        private readonly Encoding _encoding;
        private int _entrySize;

        #endregion

        #region Construction and Disposal

        public Table(TableByteRepresentation container, Schema<T> schema, Encoding encoding)
        {
            _container = container;
            _schema = schema;
            _encoding = encoding;
            _entrySize = schema.GetSize(encoding);
        }

        public void Dispose()
        {
            _container.Dispose();
        }

        #endregion

        #region ITable interface

        public long ElementCount
        {
            get
            {
                // could use some caching 
                var byteCount = _container.PrimaryContainer.ElementCount;
                Debug.Assert(byteCount % _entrySize == 0);
                return byteCount / _entrySize;
            }
        }

        public T Read(long position)
        {
            long adjustedPosition = position * _entrySize;
            return _schema.Read(_container, _encoding, ref adjustedPosition);
        }

        public void Write(long position, T newValue)
        {
            long adjustedPosition = position * _entrySize;
            long iterator = adjustedPosition;
            _schema.Write(_container, _encoding, ref iterator, newValue);
            Debug.Assert(iterator - adjustedPosition == _entrySize);
        }

        #endregion
    }

    internal static class Table
    {
        public static Table<T> Create<T>(TableByteRepresentation container, Schema<T> schema, Encoding encoding)
        {
            return new Table<T>(container, schema, encoding);
        }
    }
}
