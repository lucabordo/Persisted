using Common;

#if USE_READABLE_ENCODING 
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    /// <summary>
    /// A schema for entries that can be stored in collections
    /// </summary>

    internal abstract class Schema<T>
    {
        #region Abstract Properties and methods 

        /// <summary>
        /// Number of bytes of fixed-size storage used by any element
        /// </summary>
        internal abstract int GetFixedSize(Encoding encoding);

        /// <summary>
        /// Number of bytes of dynamic-size storage used by a specific element
        /// </summary>
        internal abstract int GetDynamicSize(Encoding encoding, T element);

        /// <summary>
        /// Extract an element of the corresponding type from a storage composed of a fixed and a dynamic part
        /// </summary>
        internal abstract T Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage, Encoding encoding, ref long position);

        /// <summary>
        /// Insert an element of the corresponding type into a storage composed of a fixed and a dynamic part
        /// </summary>
        internal abstract void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, Encoding encoding, ref long position, T element);

        #endregion
    }
}
