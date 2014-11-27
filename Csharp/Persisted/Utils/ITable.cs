namespace Persisted.Utils
{
    /// <summary>
    /// A container of elements that are read and written individually, synchronously
    /// </summary>
    public interface ITable<Element>
    {
        /// <summary>
        /// Number of elements currently stored
        /// </summary>
        long ElementCount { get; }

        /// <summary>
        /// Access an individual element
        /// </summary>
        Element Read(long position);

        /// <summary>
        /// Write an individual element.
        /// Writing is either allowed at a position strictly less than <see cref="ElementCount"/>, which is an overwrite;
        /// or at position exactly <see cref="ElementCount"/> which is an addition of new element.
        /// </summary>
        void Write(long position, Element newValue);
    }
}
