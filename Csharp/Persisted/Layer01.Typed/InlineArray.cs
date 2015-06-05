using System;

namespace Persisted.Typed
{
    internal class InlineArraySchema<T> : Schema<T[]>
    {
        #region Fields and Construction

        internal readonly Schema<T> S;

        internal InlineArraySchema(Schema<T> s)
        {
            S = s;
        }

        #endregion)


        // This just combines the complications of string and Tuple, but nothing crazzy:
        // 

        internal override int GetSize(Encoding encoding)
        {
            return encoding.EncodingSizeForReservedChar + encoding.EncodingSizeForLong + encoding.EncodingSizeForInt;
        }

        internal override bool IsFixedSized
        {
            get { return false; }
        }

        internal override T[] Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            // The schema S might be a tuple which expects a TableByteRepresentation
            // We need a TableByteRepresentation adapter? How? 
            // This should have a shift...
            throw new NotImplementedException();
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, T[] element)
        {
            throw new NotImplementedException();
        }
    }
}
