using System;
using System.Threading.Tasks;

namespace Persisted.Utils
{
    /// <summary>
    /// A container of elements that are read and written by blocks, asynchronously
    /// </summary>
    internal interface IContainerBase : IDisposable
    {
        /// <summary>
        /// Number of elements stored in each block of the container
        /// </summary>
        int BlockSize { get; }

        /// <summary>
        /// Number of blocks stored in the container
        /// </summary>
        long BlockCount { get; }
    }


    /// <summary>
    /// A container of elements that are read and written by blocks, asynchronously
    /// </summary>
    internal interface IContainer<Element> : IContainerBase
    {
        /// <summary>
        /// Read a block of <see cref="BlockSize"/> elements of type <typeparamref name="Element"/> from the container
        /// </summary>
        /// <param name="position">
        ///   The index of an existing block, between 0 and <see cref="BlockCount"/>, excluded.
        /// </param>
        /// <param name="buffer">
        ///   An array of elements of size <see cref="BlockSize"/>.
        /// </param>
        Task ReadBlock(long position, Element[] buffer);

        /// <summary>
        /// Write a block of <see cref="BlockSize"/> elements of type <typeparamref name="Element"/> into the container.
        /// The write can be at an existing position between 0 and the <see cref="BlockCount"/>, excluded, 
        /// in which case the existing content is overriden;
        /// OR, the write can be at position exactly <see cref="BlockCount"/>, in which case a new block is created.
        /// </summary>
        /// <param name="position">
        ///   The index between 0 and <see cref="BlockCount"/> excluded of an existing block to overwrite,
        ///   or exactly <see cref="BlockCount"/> to indicate that we extend the storage with a new block.
        /// </param>
        /// <param name="buffer">
        ///   An array of elements of size <see cref="BlockSize"/>. 
        /// </param>
        Task WriteBlock(long position, Element[] buffer);

        /// <summary>
        /// One block of free space allocated at the beginning of every every container allowing for the storage
        /// of some limited amount of annotations - size is exactly <see cref="BlockSize"/>.
        /// </summary>
        /// <remarks>
        /// It's fine to directly write there, the header is always saved on disposal
        /// </remarks>
        byte[] Header { get; }
    }
}
