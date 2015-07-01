using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Common;

namespace Common.Test
{
    [TestClass]
    public class UnitTests
    {
        [TestMethod]
        public void TestByteBufferBasics()
        {
            var buffer = new ByteBuffer(1);
            Assert.AreEqual(1, buffer.Capacity);

            buffer.Resize(5, ignoreContent: true);
            Assert.AreEqual(5, buffer.Capacity);

            var read = buffer.GetWriteView(1, 3);
            read[0] = (byte)'A';
            read[1] = (byte)'B';

            buffer.Resize(20, ignoreContent: false);
            var write = buffer.GetReadView(1, 3);
            Assert.AreEqual((byte)'A', write[0]);
            Assert.AreEqual((byte)'B', write[1]);
        }

        [TestMethod]
        public void TestByteBufferBatchFull()
        {
            var buffer = new ByteBuffer(100);
            var truth = Enumerable.ToArray(from i in Enumerable.Range(0, 10) select (byte)('0' + i));
            var result = new byte[10];

            var write = buffer.GetWriteView(10, 20);
            Assert.AreEqual(10, write.Count);
            write.Write(0, truth, 0, 10);

            var read = buffer.GetReadView(10, 20);
            Assert.AreEqual(10, read.Count);
            read.Read(0, result, 0, 10);

            for (int i = 0; i < 10; ++i)
            {
                Assert.AreEqual((byte)('0' + i), result[i]);
            }
        }

        [TestMethod]
        public void TestByteBufferBatchPartial()
        {
            var buffer = new ByteBuffer(100);
            var truth = Enumerable.ToArray(from i in Enumerable.Range(0, 10) select (byte)('0' + i));
            var result = new byte[100];

            var write = buffer.GetWriteView(5, 20);
            Assert.AreEqual(15, write.Count);
            write.Write(5, truth, 3, 5); // write the bytes 3, 4, 5, 6, 7 into buffer's position 10...

            var read = buffer.GetReadView(2, 32);
            Assert.AreEqual(30, read.Count);
            read.Read(8, result, 17, 5); // copy these 5 bytes into result's positions 17...

            for (int i = 0; i < 5; ++i)
            {
                Assert.AreEqual((byte)('3' + i), result[17 + i]);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions1()
        {
            var buffer = new ByteBuffer(100);
            var write = buffer.GetWriteView(5, 20);
            write[-1] = (byte)0;
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions2()
        {
            var buffer = new ByteBuffer(100);
            var write = buffer.GetWriteView(5, 20);
            Assert.AreEqual(15, write.Count);
            write[write.Count] = (byte)0;
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions3()
        {
            var buffer = new ByteBuffer(100);
            var read = buffer.GetReadView(5, 20);
            byte b = read[-1];
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions4()
        {
            var buffer = new ByteBuffer(100);
            var read = buffer.GetReadView(5, 20);
            Assert.AreEqual(15, read.Count);
            byte b = read[read.Count];
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions5()
        {
            var buffer = new ByteBuffer(100);
            var other = new byte[100];
            var write = buffer.GetWriteView(5, 20);
            write.Write(-1, other, 10, 2);
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions6()
        {
            var buffer = new ByteBuffer(100);
            var other = new byte[100];
            var write = buffer.GetWriteView(5, 20);
            write.Write(write.Count, other, 10, 2);
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions7()
        {
            var buffer = new ByteBuffer(100);
            var other = new byte[100];
            var read = buffer.GetReadView(5, 20);
            read.Read(-1, other, 10, 2);
        }

        [TestMethod]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void TestByteBufferExceptions8()
        {
            var buffer = new ByteBuffer(100);
            var other = new byte[100];
            var read = buffer.GetReadView(5, 20);
            read.Read(read.Count, other, 10, 2);
        }
    }
}
