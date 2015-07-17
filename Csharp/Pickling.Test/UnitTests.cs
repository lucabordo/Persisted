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
        public void TestPiclingLong1()
        {
            var schema = Pickling.Schema.Long;

            var index = new InMemoryByteContainer();
            var data = new InMemoryByteContainer();

            var table = Table.Create(schema, index, data);

            table.Write(0, 123);

            var chec = table.Read(0);

            //var storage =
                
            //    new Persisted.Typed.TableByteRepresentation(
            //       new TableFromListContainer<byte>(9, 4),
            //       new TableFromListContainer<byte>(9, 4));

            //var schema = Schema.Long;
            //var data = new[]
            //            {
            //                (long)43,
            //                (long)-12,
            //            };

            //schema.Write(storage, 0, data);
            //var results = schema.Read(storage, 0, data.Length).ToArray();

            //for (int i = 0; i < data.Length; ++i)
            //{
            //    Assert.AreEqual(data[i], results[i]);
            //}
        }

        #endregion
    }
}
