using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace Common.Test
{
    [TestClass]
    public class UnitTests
    {
        #region ByteBuffer

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

        #endregion

        #region Cache

        [TestMethod]
        public void TestGenericCache1()
        {
            var evictions = new List<int>();
            var cache = new Cache<int, int>(
                5,
                x => x,
                (x, _) => evictions.Add(x));

            {
                int i = cache[0];
                Assert.AreEqual(0, i);
            }
            // 0

            {
                int i = cache[0];
                Assert.AreEqual(0, i);
            }
            // 0

            {
                int i = cache[1];
                Assert.AreEqual(1, i);
            }
            // 10

            {
                int i = cache[1];
                Assert.AreEqual(1, i);
            }
            // 10

            {
                int i = cache[0];
                Assert.AreEqual(0, i);
            }
            // 01

            {
                int i = cache[1];
                Assert.AreEqual(1, i);
            }
            // 10

            {
                int i = cache[2];
                Assert.AreEqual(2, i);
            }
            // 210

            {
                int i = cache[3];
                Assert.AreEqual(3, i);
            }
            // 3210

            {
                int i = cache[4];
                Assert.AreEqual(4, i);
            }
            // 43210

            {
                int i = cache[0];
                Assert.AreEqual(0, i);
            }
            // 04321

            {
                int i = cache[1];
                Assert.AreEqual(1, i);
            }
            // 10432


            Assert.AreEqual(0, evictions.Count);

            {
                int i = cache[7];
                Assert.AreEqual(7, i);
            }
            // 71043

            Assert.AreEqual(1, evictions.Count);
            Assert.AreEqual(2, evictions[0]);

            {
                int i = cache[8];
                Assert.AreEqual(8, i);
            }
            // 87104

            Assert.AreEqual(2, evictions.Count);
            Assert.AreEqual(3, evictions[1]);

            {
                int i = cache[1];
                Assert.AreEqual(1, i);
            }
            // 18704

            Assert.AreEqual(2, evictions.Count);
            Assert.AreEqual(3, evictions[1]);

            {
                int i = cache[9];
                Assert.AreEqual(9, i);
            }
            // 91870

            Assert.AreEqual(3, evictions.Count);
            Assert.AreEqual(4, evictions[2]);

            {
                int i = cache[0];
                Assert.AreEqual(0, i);
            }
            // 09187

            Assert.AreEqual(3, evictions.Count);
            Assert.AreEqual(4, evictions[2]);

            {
                int i = cache[4];
                Assert.AreEqual(4, i);
            }
            // 40918

            Assert.AreEqual(4, evictions.Count);
            Assert.AreEqual(7, evictions[3]);

            {
                int i = cache[8];
                Assert.AreEqual(8, i);
            }
            // 84091

            Assert.AreEqual(4, evictions.Count);
            Assert.AreEqual(7, evictions[3]);

            {
                int i = cache[5];
                Assert.AreEqual(5, i);
            }
            // 58409

            Assert.AreEqual(5, evictions.Count);
            Assert.AreEqual(1, evictions[4]);

            {
                int i = cache[6];
                Assert.AreEqual(6, i);
            }
            // 65840

            Assert.AreEqual(6, evictions.Count);
            Assert.AreEqual(9, evictions[5]);
        }

        [TestMethod]
        public void TestGenericCache2()
        {
            var prng = new Random(123);
            var evictions = new List<int>();
            var cache = new Cache<int, int>(
                5,
                x => x,
                (x, _) => evictions.Add(x));

            for (int i = 0; i < 10000; ++i)
            {
                var rand = prng.Next(20);
                int next = cache[rand];
                Assert.AreEqual(next, rand);
            }
        }

        #endregion

        #region Encoding

        [TestMethod]
        public void TestEncoding()
        {
            byte b1;
            byte b2;

            Converter16.ToBytes('k', out b1, out b2);
            Assert.AreEqual('k', Converter16.FromBytes(b1, b2));

            var bytes = new byte[6];
            Converter16.ToBytes((char)'A', out bytes[0], out bytes[1]);
            Converter16.ToBytes((char)UInt16.MaxValue, out bytes[2], out bytes[3]);
            Converter16.ToBytes((char)'9', out bytes[4], out bytes[5]);

            var path = Path.GetTempFileName();
            try
            {
                File.WriteAllBytes(path, bytes);

                using (var stream = new StreamReader(path, System.Text.Encoding.Unicode))
                {
                    var l = stream.ReadLine();
                    Assert.AreEqual('A', l[0]);
                    Assert.AreEqual('9', l[2]);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [TestMethod]
        public void TestConversions1()
        {
            foreach (var x in new[] { -1.034e32, 0.467, 1239.033, 0, double.MinValue, double.MaxValue, double.NegativeInfinity, double.PositiveInfinity, double.NaN })
            {
                var binary = Converter64.LongFromDouble(x);
                var back = Converter64.DoubleFromLong(binary);
                Assert.AreEqual(x, back);
            }

            foreach (var x in new[] { DateTime.Now, DateTime.MinValue, DateTime.MaxValue })
            {
                var binary = Converter64.LongFromDateTime(x);
                var back = Converter64.DateTimeFromLong(binary);
                Assert.AreEqual(x, back);
            }

            foreach (long value in new[] { 94775L, long.MinValue, long.MaxValue, 0L })
            {
                byte[] bytes = new byte[8];
                {
                    var converter = new Converter64();
                    converter.AsLong = value;
                    bytes[0] = converter.Byte0;
                    bytes[1] = converter.Byte1;
                    bytes[2] = converter.Byte2;
                    bytes[3] = converter.Byte3;
                    bytes[4] = converter.Byte4;
                    bytes[5] = converter.Byte5;
                    bytes[6] = converter.Byte6;
                    bytes[7] = converter.Byte7;
                }
                {
                    var converter = new Converter64();
                    converter.Byte0 = bytes[0];
                    converter.Byte1 = bytes[1];
                    converter.Byte2 = bytes[2];
                    converter.Byte3 = bytes[3];
                    converter.Byte4 = bytes[4];
                    converter.Byte5 = bytes[5];
                    converter.Byte6 = bytes[6];
                    converter.Byte7 = bytes[7];
                    Assert.AreEqual(value, converter.AsLong);
                }
            }
        }

        #endregion
    }
}
