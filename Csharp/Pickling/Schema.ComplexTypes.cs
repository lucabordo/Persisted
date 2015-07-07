using System;
using Common;

#if USE_READABLE_Encoding
    using Encoding = Pickling.ReadableEncoding;
#else
    using Encoding = Pickling.ReadableEncoding;
#endif

namespace Pickling
{
    internal class StringSchema : Schema<string>
    {
        // TODO: schemas are stateful, therefore construct explicitly
        private char[] buffer_ = new char[128];

        internal override int GetFixedSize()
        {
            return Encoding.EncodingSizeForOffset;
        }

        internal override int GetDynamicSize(string element)
        {
            return Encoding.EncodingSizeForString(element.Length);
        }

        internal override string Read(ByteSegmentReadView fixedStorage, ByteSegmentReadView dynamicStorage)
        {
            int length = Encoding.ReadInt(fixedStorage);
            return Encoding.ReadString(dynamicStorage, length, ref buffer_);
        }

        internal override void Write(ByteSegmentWriteView fixedStorage, ByteSegmentWriteView dynamicStorage, string element)
        {
            Encoding.WriteInt(fixedStorage, element.Length);
            Encoding.WriteString(dynamicStorage, element);
        }
    }
}
