using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using NUnit.Framework;

namespace SQLite.Tests
{
    [TestFixture]
    public class EnumNullableTests
    {
        public enum TestEnum
        {
            Value1,

            Value2,

            Value3
        }

        public class TestObj
        {
            [PrimaryKey]
            public int Id { get; set; }
            public TestEnum? Value { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Value={1}]", Id, Value);
            }

        }

        private SQLiteConnection GetConnection() {
            var db = TestDb.GetMemoryDb();
            var response = db.CreateTable<TestObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            return db;
        }

        [Test]
        public void ShouldPersistAndReadEnum()
        {
            var db = GetConnection();

            var obj1 = new TestObj() { Id = 1, Value = TestEnum.Value2 };
            var obj2 = new TestObj() { Id = 2, Value = TestEnum.Value3 };

            var numIn1 = db.Insert(obj1);
            var numIn2 = db.Insert(obj2);
            Assert.AreEqual(1, numIn1);
            Assert.AreEqual(1, numIn2);

            var result = db.Query<TestObj>("select * from TestObj").ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(obj1.Value, result[0].Value);
            Assert.AreEqual(obj2.Value, result[1].Value);

            Assert.AreEqual(obj1.Id, result[0].Id);
            Assert.AreEqual(obj2.Id, result[1].Id);

            db.Close();
        }
    }
}
