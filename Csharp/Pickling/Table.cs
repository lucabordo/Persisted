using System;
using System.Diagnostics.Contracts;

using Common;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    public class Table<T> : IDisposable
    {
        #region Fields

        // Provided

        private readonly ByteContainer fixedContainer;
        private readonly ByteContainer dynamicContainer;
        private readonly Schema<T> schema;

        // Allocated internally

        // Cached 

        private int fixedEncodingSize;

//        private readonly ByteSegmentReadView reusedReader;
//        private readonly ByteSegmentWriteView reusedWriter;

        #endregion

        #region Construction and Disposal

        public Table(ByteContainer fixedContainer, ByteContainer dynamicContainer, Schema<T> schema)
        {
            this.fixedContainer = fixedContainer;
            this.dynamicContainer = dynamicContainer;
            this.schema = schema;
            fixedEncodingSize = schema.GetFixedSize();

            if (fixedContainer.Count % fixedEncodingSize != 0)
                throw new ArgumentException("Fixed storage should be of size multiple of the schema fixed size");
        }

        public void Dispose()
        {
            fixedContainer.Dispose();
            dynamicContainer.Dispose();
        }

        #endregion

        #region Main methods

        public long Count
        {
            get
            {
                Contract.Invariant(fixedContainer.Count % fixedEncodingSize == 0);
                return fixedContainer.Count / fixedEncodingSize;
            }
        }


        // Fields for use only in the Read method
        private readonly ByteBuffer fixedReadBuffer = new ByteBuffer();
        private readonly ByteBuffer dynamicReadBuffer = new ByteBuffer();

        public T Read(long position)
        {
//            var dynamicSize = schema.GetDynamicSize();
            fixedReadBuffer.Resize(fixedEncodingSize);
            
            var fixedReader = fixedReadBuffer.GetReadView(0, fixedEncodingSize);
            //var dynamicReader = 

            return default(T);
        }


        // Fields for use only in the Write method
        private readonly ByteBuffer fixedWriteBuffer = new ByteBuffer();
        private readonly ByteBuffer dynamicWriteBuffer = new ByteBuffer();

        // Start by this: APPEND THAT ALLOWS EFFICIENT READ.
        public void Append(T element)
        {
            // Positions
            long fixedStart = (fixedEncodingSize + Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt);
            long end = dynamicContainer.Count;

            // Encoding 
            var dynamicEncodingSize = schema.GetDynamicSize(element);

            fixedWriteBuffer.Resize(fixedEncodingSize + Encoding.EncodingSizeForLong + Encoding.EncodingSizeForInt);
            dynamicWriteBuffer.Resize(dynamicEncodingSize);

            var fixedWriter = fixedWriteBuffer.GetWriteView(0, fixedEncodingSize);
            var dynamicWriter = dynamicWriteBuffer.GetWriteView(0, dynamicEncodingSize);

            schema.Write(fixedWriter, dynamicWriter, element);

            // Emplace the buffers into the container
            fixedContainer.Write(fixedWriter, 0);
            dynamicContainer.Write(dynamicWriter, 0);
        }

        public void Write(long position, T element)
        {
            // Even here we need to locate the right STARTING POINT IN dynamic storage
        }

        #endregion
    }
}
