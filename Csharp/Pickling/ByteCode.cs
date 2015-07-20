using Common;
using System.Reflection.Emit;

namespace Pickling
{
    internal interface ICompiledSchema<T>
    {
        /// <summary>
        /// Extract an element of the corresponding type from a storage composed of a fixed and a dynamic part
        /// </summary>
        T Read(ByteBufferReadCursor segment);
    }
}
