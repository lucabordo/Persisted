using System;
using Pickling;
using Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Pickling.Test
{
    [TestClass]
    public class UnitTests
    {
        #region Basic pickling tests

        [TestMethod]
        public void TestEncodeDecodeString()
        {
            var buffer = new ByteBuffer(1000);

            var testString = "Hello World!";
            var encodingSize = ReadableEncoding.EncodingSizeForChar * testString.Length;

            ReadableEncoding.WriteString(buffer.GetWriteCursor(0, encodingSize), testString);
            var characters = new char[1000];
            var check = ReadableEncoding.ReadString(buffer.GetReadCursor(0, encodingSize), testString.Length, ref characters);
            Assert.AreEqual(testString, check);
        }

        [TestMethod]
        public void TestEncodeDecodeLong()
        {
            var buffer = new ByteBuffer(1000);

            var testNumber = -14333;
            var encodingSize = ReadableEncoding.EncodingSizeForInt;

            ReadableEncoding.WriteInt(buffer.GetWriteCursor(0, encodingSize), testNumber);
            var check = ReadableEncoding.ReadInt(buffer.GetReadCursor(0, encodingSize));
            Assert.AreEqual(testNumber, check);
        }

        [TestMethod]
        public void TestPiclingLong1()
        {
            var schema = Pickling.Schema.Long;

            var index = new InMemoryByteContainer();
            var data = new InMemoryByteContainer();

            var table = Table.Create(schema, index, data);

            table.Write(0, 123);
            var check = table.Read(0);
            Assert.AreEqual(123, check);
        }

        [TestMethod]
        public void TestPiclingString()
        {
            var schema = Pickling.Schema.String();

            var index = new InMemoryByteContainer();
            var data = new InMemoryByteContainer();

            var table = Table.Create(schema, index, data);
            const string example = "Hello, World!";

            table.Write(0, example);
            var check = table.Read(0);
            Assert.AreEqual(example, check);
        }

        [TestMethod]
        public void TestPiclingArray1()
        {
            var schema = Pickling.Schema.Array(Pickling.Schema.Int);

            var index = new InMemoryByteContainer();
            var data = new InMemoryByteContainer();

            var table = Table.Create(schema, index, data);
            var example = new int[] { 1, 2, 3, 4 };

            table.Write(0, example);
            var check = table.Read(0);
            Assert.AreEqual(example.Length, check.Length);
            for (int i = 0; i < example.Length; ++i)
                Assert.AreEqual(check[i], example[i]);
        }

        #endregion
    }
}
