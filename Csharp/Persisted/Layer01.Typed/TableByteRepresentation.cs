using Persisted.Utils;
using System;
using System.Runtime.CompilerServices;

namespace Persisted.Typed
{
    /// <summary>
    /// A group of tables of bytes that provide the low-level, persisted representation
    /// in bytes of a structured table
    /// </summary>
    internal struct TableByteRepresentation: IDisposable
    {
        #region Constants and Fields 

        internal const char ElementSeparator = ',';
        internal const char EndOfTuple = '\0';

        private TableFromContainer<byte> _primaryContainer;
        private TableFromContainer<byte> _secondaryContainer;

        #endregion

        #region Construction

        public TableByteRepresentation(TableFromContainer<byte> primary, TableFromContainer<byte> secondary)
        {
            _primaryContainer = primary;
            _secondaryContainer = secondary;
        }

        public void Dispose()
        {
            if (_primaryContainer != null)
                _primaryContainer.Dispose();
            if (_secondaryContainer != null)
                _secondaryContainer.Dispose();
            _primaryContainer = _secondaryContainer = null;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// A container that stores tuples of a fized size, allowing random access
        /// </summary>
        public TableFromContainer<byte> PrimaryContainer
        {
            get { return _primaryContainer; }
        }

        /// <summary>
        /// A container for any arbitrary-size elements
        /// </summary>
        public TableFromContainer<byte> SecondaryContainer
        {
            get { return _secondaryContainer; }
        }

        #endregion
    }
}
