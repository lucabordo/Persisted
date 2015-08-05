using System;
using System.Diagnostics.Contracts;
using System.Threading;

#if USE_REFLECTION_EMIT
    using System.Reflection;
    using System.Reflection.Emit;
#endif

using Common;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    public static class Table
    {
        public static Table<T> Create<T>(Schema<T> schema, ByteContainer indexContainer, ByteContainer dataContainer)
        {
            return new Table<T>(schema, indexContainer, dataContainer);
        }
    }


    /// <summary>
    /// A contiguous, random access collection of elements of a certain Schema,
    /// transparently serialized into two ByteContainers.
    /// </summary>
    public class Table<T> : IDisposable
    {
        #region Provided fields

        /// <summary>
        /// The schema of the contained elements
        /// </summary>
        private readonly Schema<T> schema;

        /// <summary>
        /// A container that contains a number of contiguous elements
        /// </summary>
        private readonly ByteContainer data;

        /// <summary>
        /// The list of starting point and sizes in the data container
        /// </summary>
        private readonly ByteContainer index;

        #endregion

        #region Cached data

        /// <summary>
        /// If the schema is fixed size we keep its size
        /// </summary>
        private Nullable<int> FixedSize;

        #endregion

        #region Construction and Disposal

        public Table(Schema<T> schema, ByteContainer indexContainer, ByteContainer dataContainer)
        {
            // Provided fields
            this.index = indexContainer;
            this.data = dataContainer;
            this.schema = schema;

            // Cached data
            FixedSize = (schema.IsFixedSize)
                ? FixedSize = schema.GetDynamicSize(default(T))
                : null;

            // Buffer allocations
            Buffer = new ByteBuffer(IndexEntryEncodingSize);

            BufferReader = Buffer.GetReadCursor();
            BufferWriter = Buffer.GetWriteCursor();

            BufferStorer = Buffer.GetBlockReader();
            BufferLoader = Buffer.GetBlockWriter();

            // Checks
            //          if (indexContainer.Count % fixedEncodingSize != 0)
            //            throw new ArgumentException("Fixed storage should be of size multiple of the schema fixed size");
        }

        public void Dispose()
        {
            index.Dispose();
            data.Dispose();
        }

        #endregion

        #region Access to Index storage

        /// <summary>
        /// Size required to encode a object of type <see cref="IndexEntry"/>
        /// </summary>
        private static readonly int IndexEntryEncodingSize = Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt;

        /// <summary>
        /// Entry stored in the Index,
        /// represents the exact byte positions where an object is encoded in the data storage.
        /// </summary>
        /// <remarks>
        /// This allows in theory constant-time lookup of each stored object's representation
        /// </remarks>
        private struct IndexEntry
        {
            /// <summary>
            /// Start of object encoding in data storage
            /// </summary>
            public long Start;

            /// <summary>
            /// Length of object encoding in data storage
            /// </summary>
            public int Length;
        }

        // All reading and writing goes to the containers through a buffer
        // The buffer is reused because this class runs single-threaded (check).

        // The table reads and writes the buffer using a reader and writer
        // The containers use another pair of reader/writers. THESE SHOULD HAVE DIFFERENT TYPE (load/store)

        private ByteBufferReadCursor BufferReader;
        private ByteBufferWriteCursor BufferWriter;

        private readonly ByteBuffer Buffer;

        private ByteBufferBlockReader BufferStorer;
        private ByteBufferBlockWriter BufferLoader;

        private IndexEntry ReadIndex(long position)
        {
            // Bounds checking

            // Reset the buffers
            Contract.Requires(Buffer.Capacity > IndexEntryEncodingSize);
            Buffer.Reset(BufferReader, IndexEntryEncodingSize);
            Buffer.Reset(BufferLoader, IndexEntryEncodingSize);

            // Load segment from storage
            index.Load(BufferLoader, position * IndexEntryEncodingSize);

            // Decode segment
            long start = Encoding.ReadLong(BufferReader);
            int length = Encoding.ReadInt(BufferReader);
            return new IndexEntry { Start = start, Length = length };
        }

        private void WriteIndex(long position, IndexEntry entry)
        {
            // Bounds checking

            // Reset the buffers
            Contract.Requires(Buffer.Capacity > IndexEntryEncodingSize);
            Buffer.Reset(BufferWriter, IndexEntryEncodingSize);
            Buffer.Reset(BufferStorer, IndexEntryEncodingSize);

            // Encode segment
            Encoding.WriteLong(BufferWriter, entry.Start);
            Encoding.WriteInt(BufferWriter, entry.Length);

            // Store the segment into storage
            index.Store(BufferStorer, position * IndexEntryEncodingSize);
        }

        #endregion

        #region Access to Data storage

        private T ReadData(long start, int size)
        {
            // Reset the buffers
            Buffer.Resize(size);
            Buffer.Reset(BufferReader, size);
            Buffer.Reset(BufferLoader, size);

            // Load segment from storage
            data.Load(BufferLoader, start);

            // Decode segment
            if (compiledSchema != null)
                return compiledSchema.Read(BufferReader);
            else
                return schema.Read(BufferReader);
        }

        private void WriteData(long start, T element, int size)
        {
            // Reset the buffers
            Buffer.Resize(size);
            Buffer.Reset(BufferWriter, size);
            Buffer.Reset(BufferStorer, size);

            // Encode segment
            schema.Write(BufferWriter, element);

            // Store the segment into storage
            data.Store(BufferStorer, start);
        }

        #endregion

        #region Main methods

        /// <summary>
        /// Get the number of stored elements
        /// </summary>
        public long Count
        {
            get
            {
                Contract.Invariant(index.Count % IndexEntryEncodingSize == 0);
                return index.Count / IndexEntryEncodingSize;
            }
        }

        /// <summary>
        /// Read the <paramref name="position"/>-th stored element
        /// </summary>
        public T Read(long position)
        {
            if (FixedSize.HasValue)
            {
                // When the encoding has fixed size we never write the index, and have no fragmentation 
                return ReadData(position * FixedSize.Value, FixedSize.Value);
            }
            else
            {
                IndexEntry index = ReadIndex(position);
                return ReadData(index.Start, index.Length);
            }
        }

        /// <summary>
        /// Overwrite the data stored at the given position, or
        /// if the <code>position == Count</code>, create a new position
        /// </summary>
        public void Write(long position, T element)
        {
            if (FixedSize.HasValue)
            {
                // When the encoding has fixed size we never write the index, and have no fragmentation 
                WriteData(position * FixedSize.Value, element, FixedSize.Value);
            }
            else
            {
                long start = data.Count;
                int size = schema.GetDynamicSize(element);

                WriteIndex(position, new IndexEntry { Start = start, Length = size });
                WriteData(start, element, size);
            }

            // TODO: Some assertions here!!

            // TODO: Keep track of fragmentation. This would require an index read before write...
        }

        #endregion

        #region Experiments with Reflection.Emit. This will move
        
        // The goal is to produce this: 
        private ICompiledSchema<T> compiledSchema;

        private void CompileReadMethod(TypeBuilder typeBuilder)
        {
            var peek1 = typeof(T);
            var methodBuilder = typeBuilder.DefineMethod(
                "Read",
                MethodAttributes.Public | MethodAttributes.Virtual,
                typeof(T),
                new Type[] { typeof(ByteBufferReadCursor) });

            var generator = methodBuilder.GetILGenerator();
            schema.CompileReadMethod(generator);
            var toto = methodBuilder.Signature;
        }

        private Tuple<int, byte> SomeInterestingIL(ByteBufferReadCursor typeBuilder)
        {
            return new Tuple<int, byte>(
                Encoding.ReadInt(typeBuilder),
                Encoding.ReadByte(typeBuilder));
        }

        public void CompileSchema()
        {
            var assemblyBuilder = Thread.GetDomain().DefineDynamicAssembly(
                new AssemblyName("Persisted.Collections.EmittedTypes"),
                AssemblyBuilderAccess.Run);

            var moduleBuilder = assemblyBuilder.DefineDynamicModule("EmittedSchemas");

            var typeBuilder =  moduleBuilder.DefineType(
                "CompiledSchema", 
                TypeAttributes.Public);
            typeBuilder.AddInterfaceImplementation(typeof(ICompiledSchema<T>));

            // Read method 
            CompileReadMethod(typeBuilder);

            // Create the type, 
            var theType = typeBuilder.CreateType();
            compiledSchema = (ICompiledSchema<T>)Activator.CreateInstance(theType);
        }

        #endregion
    }
}
