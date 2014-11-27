using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using System.IO;
using Persisted;
using System.Linq;
using Persisted.Utils;
using System.Threading.Tasks;
using Persisted.Typed;
using System.Collections.Generic;
using Persisted.Bytes;
using System.Threading;

namespace PersistedTests
{
    internal static class Extensions
    {
        public static async Task<byte[]> ReadBlock(this IBlockStorage storage, ByteContainerHandle container, long position)
        {
            var buffer = new byte[storage.GetBlockSize(container)];
            await storage.ReadBlock(container, position, buffer);
            return buffer;
        }

        public static Task<Element[]> ReadBlock<Element>(this IContainer<Element> container, long position)
        {
            var buffer = new Element[container.BlockSize];
            return container.ReadBlock(position, buffer).ContinueWith(_ => buffer);
        }

        public static Persisted.Typed.Encoding DefaultEncoding = new Persisted.Typed.Encoding();

        public static IEnumerable<T> Read<T>(this Schema<T> schema, TableByteRepresentation image, long startPosition, int count)
        {
            return schema.Read(image, DefaultEncoding, startPosition, count);
        }

        public static T Read<T>(this Schema<T> schema, TableByteRepresentation image, ref long position)
        {
            return schema.Read(image, DefaultEncoding, ref position);
        }

        public static void Write<T>(this Schema<T> schema, TableByteRepresentation image, long position, params T[] element)
        {
            schema.Write(image, DefaultEncoding, position, element);
        }

        public static void Write<T>(this Schema<T> schema, TableByteRepresentation image, ref long position, T element)
        {
            schema.Write(image, DefaultEncoding, ref position, element);
        }

        public static int GetSize<T>(this Schema<T> schema)
        {
            return schema.GetSize(DefaultEncoding);
        }
    }


    [TestClass]
    public class UnitTests
    {
        #region Methods shared by tests

        public string GetStandardizedTempFileName()
        {
            return
                Path.Combine(Path.GetTempFileName(), "Test")
                .Substring(2)
                .Replace('.', '_')
                .Replace('\\', '/');
        }

        #endregion

        #region Test of Utils

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
                var binary = Persisted.Utils.Converter64.LongFromDouble(x);
                var back = Persisted.Utils.Converter64.DoubleFromLong(binary);
                Assert.AreEqual(x, back);
            }

            foreach (var x in new[] { DateTime.Now, DateTime.MinValue, DateTime.MaxValue })
            {
                var binary = Persisted.Utils.Converter64.LongFromDateTime(x);
                var back = Persisted.Utils.Converter64.DateTimeFromLong(binary);
                Assert.AreEqual(x, back);
            }

            foreach (long value in new[] { 94775L, long.MinValue, long.MaxValue, 0L})
            {
                byte[] bytes = new byte[8];
                {
                    var converter = new Persisted.Utils.Converter64();
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
                    var converter = new Persisted.Utils.Converter64();
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

        [TestMethod]
        public void TestGenericCache1()
        {
            var evictions = new System.Collections.Generic.List<int>();
            var cache = new Persisted.Utils.Cache<int, int>(
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
            var evictions = new System.Collections.Generic.List<int>();
            var cache = new Persisted.Utils.Cache<int, int>(
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

        [TestMethod]
        public void TestTableFromContainer1()
        {
            foreach (int cacheCapacity in new[] { 3, 5, 10, 100, 1000 })
            {
                const int size = 1000;
                var container = new ListContainer<int>(9);
                using (var table = new TableFromContainer<int>(container, cacheCapacity))
                {
                    for (int i = 0; i < size; ++i)
                    {
                        table.Write(i, i);
                    }

                    for (int i = 0; i < size; ++i)
                    {
                        var j = table.Read(i);
                        Assert.AreEqual(i, j);
                    }

                    for (int i = size - 1; i >= 0; --i)
                    {
                        var j = table.Read(i);
                        Assert.AreEqual(i, j);
                    }

                    var prng = new Random(123);

                    for (int trial = 0; trial < size; ++trial)
                    {
                        var i = prng.Next(size);
                        var j = table.Read(i);
                        Assert.AreEqual(i, j);
                    }

                    // now extend and read more

                    for (int i = size; i < 2 * size; ++i)
                    {
                        table.Write(i, i);
                    }

                    for (int trial = 0; trial < 2 * size; ++trial)
                    {
                        var i = prng.Next(size);
                        var j = table.Read(i);
                        Assert.AreEqual(i, j);
                    }

                    // increase by 1 all 

                    for (int i = 2 * size - 1; i >= 0; --i)
                    {
                        table.Write(i, table.Read(i) + 1);
                    }
                }

                var witness = container.Values[142];

                using (var table = new TableFromContainer<int>(container, cacheCapacity))
                {
                    for (int i = 0; i < 2 * size; ++i)
                    {
                        Assert.AreEqual(i + 1, table.Read(i));
                    }
                }
            }
        }

        #endregion

        #region Layer 0 Byte containers

        [TestMethod]
        public void TestPhysicalStorage1()
        {
            var path = GetStandardizedTempFileName();
            try
            {
                const int blockSize1 = 193928;
                const int blockSize2 = 13;

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    Assert.IsFalse(storage.Exists("files/toto"));
                    Assert.IsFalse(storage.Exists("files/tata"));

                    var container1 = storage.Create("files/toto", blockSize1);
                    var container2 = storage.Create("files/tata", blockSize2);

                    storage.Close(container1);
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    Assert.IsTrue(storage.Exists("files/toto"));
                    Assert.IsTrue(storage.Exists("files/tata"));

                    var container1 = storage.Open("files/toto");
                    var container2 = storage.Open("files/tata");

                    Assert.AreEqual(blockSize1, storage.GetBlockSize(container1));
                    Assert.AreEqual(blockSize2, storage.GetBlockSize(container2));

                    Assert.AreEqual(0, storage.GetBlockCount(container1));
                    Assert.AreEqual(0, storage.GetBlockCount(container2));

                    storage.WriteBlock(container1, 0, new byte[blockSize1]); // should be awaited when closing storage
                    storage.Delete(container2);
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    Assert.IsTrue(storage.Exists("files/toto"));
                    Assert.IsFalse(storage.Exists("files/tata"));

                    var container1 = storage.Open("files/toto");

                    Assert.AreEqual(blockSize1, storage.GetBlockSize(container1));
                    Assert.AreEqual(1, storage.GetBlockCount(container1));
                }
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        [TestMethod]
        public void TestPhysicalStorage2()
        {
            var path = GetStandardizedTempFileName();
            try
            {
                var prng = new Random(123);
                const int blockSize = 4096;

                var block1 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                var block2 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                var block3 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {

                    var toto = storage.Create("files/toto", blockSize);
                    storage
                        .WriteBlock(toto, 0, block1)
                        .ContinueWith(_ => storage.WriteBlock(toto, 0, block1))
                        .ContinueWith(_ => storage.WriteBlock(toto, 1, block2))
                        .ContinueWith(_ => storage.WriteBlock(toto, 0, block3))
                        .Wait();
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var toto = storage.Open("files/toto");

                    var block0Retrieved = storage.ReadBlock(toto, 0).Result;
                    var block1Retrieved = storage.ReadBlock(toto, 1).Result;

                    for (int i = 0; i < blockSize; ++i)
                    {
                        Assert.AreEqual(block3[i], block0Retrieved[i]);
                        Assert.AreEqual(block2[i], block1Retrieved[i]);
                    }
                }
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        // same as before but using the higher level Container APIs
        [TestMethod]
        public void TestPhysicalStorage3()
        {
            var path = GetStandardizedTempFileName();
            try
            {
                var prng = new Random(123);
                const int blockSize = 4096;

                var block1 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                var block2 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                var block3 = Enumerable.ToArray(
                    from i in Enumerable.Range(0, blockSize)
                    select (byte)prng.Next(256));

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    storage.Create("files/toto", blockSize);
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var containerWriter = storage.GetContainer("files/toto");
                    containerWriter.WriteBlock(0, block1);
                    containerWriter.WriteBlock(0, block1);
                    containerWriter.WriteBlock(1, block2);
                    containerWriter.WriteBlock(0, block3);
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var containerReader = storage.GetContainer("files/toto");

                    var block0Retrieved = containerReader.ReadBlock(0);
                    var block1Retrieved = containerReader.ReadBlock(1);

                    for (int i = 0; i < blockSize; ++i)
                    {
                        Assert.AreEqual(block3[i], block0Retrieved.Result[i]);
                        Assert.AreEqual(block2[i], block1Retrieved.Result[i]);
                    }
                }
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        [TestMethod]
        public void TestNameStandardization1()
        {
            var s1 = Persisted.Identifier.Standardize("/Users/Johnny/123_hello", '\\');
            Assert.IsTrue(String.Equals(s1, @"\users\johnny\123_hello", StringComparison.InvariantCulture));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestNameStandardization2()
        {
            var s1 = Persisted.Identifier.Standardize("C:/Users/Johnny//123_hello");
            Assert.IsTrue(String.Equals(s1, @"c:\users\johnny\123_hello", StringComparison.InvariantCulture));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestNameStandardization3()
        {
            var s1 = Persisted.Identifier.Standardize("C:/Users/Johnny*/123_hello.txt", '\\');
            Assert.IsTrue(String.Equals(s1, @"c:\users\johnny\123_hello.txt", StringComparison.InvariantCulture));
        }

        #endregion

        #region Layer 1 Typed containers

        // TEst plan:
        // fix bug below
        // have similar test for any tuple signature
        // have test for every basic type (non-tupled)
        // have tests with long collections
        // have test for composition of tuples
        // then do strings and obviously test them

        [TestMethod]
        public void TestLongSchema1()
        {
            var storage = new Persisted.Typed.TableByteRepresentation(
                   new TableFromListContainer<byte>(9, 4),
                   new TableFromListContainer<byte>(9, 4));

            var schema = Schema.Long;
            var data = new[] 
                        {
                            (long)43,
                            (long)-12,
                        };

            schema.Write(storage, 0, data);
            var results = schema.Read(storage, 0, data.Length).ToArray();

            for (int i = 0; i < data.Length; ++i)
            {
                Assert.AreEqual(data[i], results[i]);
            }
        }

        [TestMethod]
        public void TestLongSchema2()
        {
            var storage = new Persisted.Typed.TableByteRepresentation(
                   new TableFromListContainer<byte>(9, 4),
                   new TableFromListContainer<byte>(9, 4));

            var schema = Schema.Tuple(Schema.Long, Schema.Long);
            var data = new[] 
                        {
                            Tuple.Create((long)-12, (long)+45),
                            Tuple.Create(long.MinValue, long.MaxValue),
                        };

            schema.Write(storage, 0, data);
            var results = schema.Read(storage, 0, data.Length).ToArray();

            for (int i = 0; i < data.Length; ++i)
            {
                Assert.AreEqual(data[i].Item1, results[i].Item1);
                Assert.AreEqual(data[i].Item2, results[i].Item2);
            }
        }

        #region Read Write tests using Schema methods

        [TestMethod]
        public void TestStringSchema1()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Long, Schema.String);
                        var data = new[] 
                        {
                            Tuple.Create((long)-12, "Dans le port d'Amsterdam"),
                            Tuple.Create(long.MinValue, "Y a des marins qui chantent"),
                        };

                        schema.Write(storage, 0, data);
                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();
                        var results = schema.Read(storage, 0, data.Length).ToArray();

                        for (int i = 0; i < data.Length; ++i)
                        {
                            Assert.AreEqual(data[i].Item1, results[i].Item1);
                            Assert.AreEqual(data[i].Item2, results[i].Item2);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas2()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);
                        
                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();
                        
                        var schema = Schema.Tuple(Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45),
                            Tuple.Create(int.MinValue, int.MaxValue),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            if (disposeBetweenReadAndWrite)
                                reallocateStorage();
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            if (disposeBetweenReadAndWrite)
                                reallocateStorage();
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas3()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0),
                            Tuple.Create(int.MinValue, int.MaxValue, 78),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas4()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas5()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas6()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue, -9077),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0, 748777),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                                Assert.AreEqual(data[i].Item6, results[i].Item6);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                                Assert.AreEqual(data[i].Item6, results[i].Item6);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas7()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue, -9077, 0),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0, 748777, 12),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                                Assert.AreEqual(data[i].Item6, results[i].Item6);
                                Assert.AreEqual(data[i].Item7, results[i].Item7);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, results[i].Item1);
                                Assert.AreEqual(data[i].Item2, results[i].Item2);
                                Assert.AreEqual(data[i].Item3, results[i].Item3);
                                Assert.AreEqual(data[i].Item4, results[i].Item4);
                                Assert.AreEqual(data[i].Item5, results[i].Item5);
                                Assert.AreEqual(data[i].Item7, results[i].Item7);
                            }
                        }
                    }
                }
            }
        }

        #endregion

        #region Read/Write tests using Table

        [TestMethod]
        public void TestStringSchema1WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Long, Schema.String);
                        var data = new[] 
                        {
                            Tuple.Create((long)-12, "Dans le port d'Amsterdam"),
                            Tuple.Create(long.MinValue, "Y a des marins qui chantent"),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            table.Write(0, data[0]);
                            table.Write(1, data[1]);
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas2WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45),
                            Tuple.Create(int.MinValue, int.MaxValue),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas3WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0),
                            Tuple.Create(int.MinValue, int.MaxValue, 78),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                                Assert.AreEqual(data[i].Item3, table.Read(i).Item3);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas4WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                                Assert.AreEqual(data[i].Item3, table.Read(i).Item3);
                                Assert.AreEqual(data[i].Item4, table.Read(i).Item4);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas5WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                                Assert.AreEqual(data[i].Item3, table.Read(i).Item3);
                                Assert.AreEqual(data[i].Item4, table.Read(i).Item4);
                                Assert.AreEqual(data[i].Item5, table.Read(i).Item5);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas6WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue, -9077),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0, 748777),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                                Assert.AreEqual(data[i].Item3, table.Read(i).Item3);
                                Assert.AreEqual(data[i].Item4, table.Read(i).Item4);
                                Assert.AreEqual(data[i].Item5, table.Read(i).Item5);
                                Assert.AreEqual(data[i].Item6, table.Read(i).Item6);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TestTupleSchemas7WithTable()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();

                        var schema = Schema.Tuple(Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int, Schema.Int);
                        var data = new[] 
                        {
                            Tuple.Create(-12, +45, 0, 1866, int.MaxValue, -9077, 0),
                            Tuple.Create(int.MinValue, int.MaxValue, 78, int.MinValue, 0, 748777, 12),
                        };

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                table.Write(i, data[i]);
                            }
                        }

                        if (disposeBetweenReadAndWrite)
                            reallocateStorage();

                        using (var table = Table.Create(storage, schema, Extensions.DefaultEncoding))
                        {
                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1, table.Read(i).Item1);
                                Assert.AreEqual(data[i].Item2, table.Read(i).Item2);
                                Assert.AreEqual(data[i].Item3, table.Read(i).Item3);
                                Assert.AreEqual(data[i].Item4, table.Read(i).Item4);
                                Assert.AreEqual(data[i].Item5, table.Read(i).Item5);
                                Assert.AreEqual(data[i].Item6, table.Read(i).Item6);
                                Assert.AreEqual(data[i].Item7, table.Read(i).Item7);
                            }
                        }
                    }
                }
            }
        }

        #endregion

        // Tuples of tuples
        [TestMethod]
        public void TestdObjectsTupleComposition()
        {
            foreach (int blockSize in new[] { 8, 15, 30, 100 })
            {
                foreach (int cacheCapacity in new[] { 3, 5, 7, 15, 30, 100 })
                {
                    foreach (bool disposeBetweenReadAndWrite in new[] { false, true })
                    {
                        var primaryTable = new ListContainer<byte>(blockSize);
                        var secondaryTable = new ListContainer<byte>(blockSize);

                        var storage = default(Persisted.Typed.TableByteRepresentation);
                        Action reallocateStorage = () =>
                        {
                            storage.Dispose();
                            storage = new Persisted.Typed.TableByteRepresentation(
                               new TableFromListContainer<byte>(primaryTable, cacheCapacity),
                               new TableFromListContainer<byte>(secondaryTable, cacheCapacity));
                        };
                        reallocateStorage();
                        var schema = Schema.Tuple(
                            Schema.Tuple(Schema.Int, Schema.Int),
                            Schema.Tuple(Schema.Int, Schema.Int));
                        var data = new[] 
                        {
                            Tuple.Create(Tuple.Create(-12, +45), Tuple.Create(0, int.MaxValue)),
                        };

                        // read write from position 0
                        {
                            schema.Write(storage, 0, data);
                            var results = schema.Read(storage, 0, data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1.Item1, results[i].Item1.Item1);
                                Assert.AreEqual(data[i].Item1.Item2, results[i].Item1.Item2);
                                Assert.AreEqual(data[i].Item2.Item1, results[i].Item2.Item1);
                                Assert.AreEqual(data[i].Item2.Item2, results[i].Item2.Item2);
                            }
                        }
                        // read write from slightly different position
                        {
                            schema.Write(storage, schema.GetSize(), data);
                            var results = schema.Read(storage, schema.GetSize(), data.Length).ToArray();

                            for (int i = 0; i < data.Length; ++i)
                            {
                                Assert.AreEqual(data[i].Item1.Item1, results[i].Item1.Item1);
                                Assert.AreEqual(data[i].Item1.Item2, results[i].Item1.Item2);
                                Assert.AreEqual(data[i].Item2.Item1, results[i].Item2.Item1);
                                Assert.AreEqual(data[i].Item2.Item2, results[i].Item2.Item2);
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void Test1Layer2InCombinationWithLayer1()
        {
            var path = GetStandardizedTempFileName();
            const int blockSize = 17;
            const int cacheCapacity = 3;

            try
            {
                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var handle1 = storage.Create("test/part1", blockSize);
                    var handle2 = storage.Create("test/part2", blockSize);

                    var container1 = storage.GetContainer("test/part1");
                    var container2 = storage.GetContainer("test/part2");

                    var subtable1 = new TableFromContainer<byte>(container1, cacheCapacity);
                    var subtable2 = new TableFromContainer<byte>(container2, cacheCapacity);

                    var pair = new TableByteRepresentation(subtable1, subtable2);
                    using (var table = new Table<Tuple<int, string>>(pair, Schema.Tuple(Schema.Int, Schema.String), new Persisted.Typed.Encoding()))
                    {
                        table.Write(0, Tuple.Create(154, "toto"));
                    }
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var handle1 = storage.Open("test/part1");
                    var handle2 = storage.Open("test/part2");

                    var container1 = storage.GetContainer("test/part1");
                    var container2 = storage.GetContainer("test/part2");

                    var subtable1 = new TableFromContainer<byte>(container1, cacheCapacity);
                    var subtable2 = new TableFromContainer<byte>(container2, cacheCapacity);

                    var pair = new TableByteRepresentation(subtable1, subtable2);
                    using (var table = new Table<Tuple<int, string>>(pair, Schema.Tuple(Schema.Int, Schema.String), new Persisted.Typed.Encoding()))
                    {
                        Assert.AreEqual(1, table.ElementCount);
                        var next = table.Read(0);
                        Assert.AreEqual(154, next.Item1);
                        Assert.AreEqual("toto", next.Item2);
                    }
                }
            }
            finally
            {
                Directory.Delete(path, recursive: true);
            }
        }

        [TestMethod]
        public void Test2Layer2InCombinationWithLayer1()
        {
            var path = GetStandardizedTempFileName();
            const int blockSize = 17;
            const int cacheCapacity = 3;

            try
            {
                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var handle1 = storage.Create("test/part1", blockSize);
                    var handle2 = storage.Create("test/part2", blockSize);

                    var container1 = storage.GetContainer("test/part1");
                    var container2 = storage.GetContainer("test/part2");

                    var subtable1 = new TableFromContainer<byte>(container1, cacheCapacity);
                    var subtable2 = new TableFromContainer<byte>(container2, cacheCapacity);

                    var pair = new TableByteRepresentation(subtable1, subtable2);
                    using (var table = new Table<Tuple<int, string>>(pair, Schema.Tuple(Schema.Int, Schema.String), new Persisted.Typed.Encoding()))
                    {
                        for (int i = 0; i < 1000; ++i)
                        {
                            table.Write(i, Tuple.Create(i, i.ToString()));
                        }
                    }
                }

                using (var storage = Persisted.Bytes.LocalFileStorage.Create(path))
                {
                    var handle1 = storage.Open("test/part1");
                    var handle2 = storage.Open("test/part2");

                    var container1 = storage.GetContainer("test/part1");
                    var container2 = storage.GetContainer("test/part2");

                    var subtable1 = new TableFromContainer<byte>(container1, cacheCapacity);
                    var subtable2 = new TableFromContainer<byte>(container2, cacheCapacity);

                    var pair = new TableByteRepresentation(subtable1, subtable2);
                    var table = new Table<Tuple<int, string>>(pair, Schema.Tuple(Schema.Int, Schema.String), new Persisted.Typed.Encoding());

                    for (int i = 0; i < 1000; ++i)
                    {
                        var next = table.Read(i);
                        Assert.AreEqual(i, next.Item1);
                        Assert.AreEqual(i.ToString(), next.Item2);
                    }
                }
            }
            finally
            {
                Directory.Delete(path, recursive: true);
            }
        }

        #endregion

        #region Interface Mocks

        public class ListContainer<T> : IContainer<T>
        {
            public readonly List<T[]> Values = new List<T[]>();
            private byte[] _header;

            public long BlockCount
            {
                get { return Values.Count; }
            }

            public ListContainer(int blockSize)
            {
                _header = new byte[blockSize];
            }

            public int BlockSize
            {
                get { return _header.Length; }
            }

            public Task ReadBlock(long position, T[] buffer)
            {
                if (position == 1)
                {
                    var toto = Values[1];
                    var prout = Values[(int)position];
                }
                var source = Values[(int)position];
                return Task.Run(() =>
                {
                    Assert.AreEqual(BlockSize, buffer.Length);
                    for (int i = 0; i < BlockSize; ++i) 
                        buffer[i] = source[i];
                });
            }

            public Task WriteBlock(long position, T[] result)
            {
                return Task.Run(() =>
                {
                    Assert.AreEqual(BlockSize, result.Length);
                    if (position == Values.Count)
                        Values.Add(new T[BlockSize]);
                    var target = Values[(int)position];
                    for (int i = 0; i < BlockSize; ++i)
                        target[i] = result[i];
                });
            }

            public byte[] Header
            {
                get { return _header; }
            }

            public void Dispose()
            {
            }
        }

        internal class TableFromListContainer<T> : TableFromContainer<T>
        {
            public ListContainer<T> Container;

            public TableFromListContainer(ListContainer<T> container, int cacheCapacity = 3)
                : base(container, cacheCapacity)
            {
                Container = container;
            }

            public TableFromListContainer(int blockSize = 7, int cacheCapacity = 3)
                : this(new ListContainer<T>(blockSize), cacheCapacity)
            {
            }
        }

        #endregion
    }
}
