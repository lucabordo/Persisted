namespace Persisted.Typed
{
    /// <summary>
    /// A schema component for strings of unbounded size (up to int.MaxValue)
    /// </summary>
    internal class StringSchema : Schema<string>
    {
        internal override int GetSize(Encoding encoding)
        {
            return encoding.EncodingSizeForReservedChar + encoding.EncodingSizeForLong + encoding.EncodingSizeForInt;
        }

        internal override bool IsFixedSized
        {
            get { return false; }
        }

        internal override string Read(TableByteRepresentation image, Encoding encoding, ref long position)
        {
            var primary = image.PrimaryContainer;
            var secondary = image.SecondaryContainer;

            long positionInSecondarStorage = encoding.ReadReference(primary, ref position);
            int length = (int)encoding.ReadInt(primary, ref position);

            return encoding.ReadString(secondary, ref positionInSecondarStorage, length);
        }

        internal override void Write(TableByteRepresentation image, Encoding encoding, ref long position, string element)
        {
            var primary = image.PrimaryContainer;
            var secondary = image.SecondaryContainer;

            long positionInSecondaryStorage = secondary.ElementCount;
            int length = element.Length;

            encoding.WriteReference(primary, ref position, positionInSecondaryStorage);
            encoding.WriteInt(primary, ref position, length);

            encoding.WriteString(secondary, element, ref positionInSecondaryStorage);
        }
    }
}