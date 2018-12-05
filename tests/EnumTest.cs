using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using NUnit.Framework;

namespace SQLite.Tests
{
    [TestFixture]
    public class EnumTests
    {
        public enum TestEnum
        {
            Value1,

            Value2,

            Value3
        }

		[StoreAsText]
		public enum StringTestEnum
		{
			Value1,

			Value2,

			Value3
		}

		public class TestObj
        {
            [PrimaryKey]
            public int Id { get; set; }
            public TestEnum Value { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Value={1}]", Id, Value);
            }

        }

		public class StringTestObj
		{
			[PrimaryKey]
			public int Id { get; set; }
			public StringTestEnum Value { get; set; }

			public override string ToString ()
			{
				return string.Format("[StringTestObj: Id={0}, Value={1}]", Id, Value);
			}

		}

        private SQLiteConnection GetConnection() {
            var db = TestDb.GetMemoryDb();
            var response = db.CreateTable<TestObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            response = db.CreateTable<StringTestObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            response = db.CreateTable<ByteTestObj>();
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

		[Test]
		public void ShouldPersistAndReadStringEnum ()
		{
            var db = GetConnection();

			var obj1 = new StringTestObj() { Id = 1, Value = StringTestEnum.Value2 };
			var obj2 = new StringTestObj() { Id = 2, Value = StringTestEnum.Value3 };

			var numIn1 = db.Insert(obj1);
			var numIn2 = db.Insert(obj2);
			Assert.AreEqual(1, numIn1);
			Assert.AreEqual(1, numIn2);

			var result = db.Query<StringTestObj>("select * from StringTestObj").ToList();
			Assert.AreEqual(2, result.Count);
			Assert.AreEqual(obj1.Value, result[0].Value);
			Assert.AreEqual(obj2.Value, result[1].Value);

			Assert.AreEqual(obj1.Id, result[0].Id);
			Assert.AreEqual(obj2.Id, result[1].Id);

			db.Close();
		}

		public enum ByteTestEnum : byte
		{
			Value1 = 1,

			Value2 = 2,

			Value3 = 3
		}

		public class ByteTestObj
		{
			[PrimaryKey]
			public int Id { get; set; }
			public ByteTestEnum Value { get; set; }

			public override string ToString ()
			{
				return string.Format ("[ByteTestObj: Id={0}, Value={1}]", Id, Value);
			}
		}

		[Test]
		public void Issue33_ShouldPersistAndReadByteEnum ()
		{
            var db = GetConnection();

			var obj1 = new ByteTestObj () { Id = 1, Value = ByteTestEnum.Value2 };
			var obj2 = new ByteTestObj () { Id = 2, Value = ByteTestEnum.Value3 };

			var numIn1 = db.Insert (obj1);
			var numIn2 = db.Insert (obj2);
			Assert.AreEqual (1, numIn1);
			Assert.AreEqual (1, numIn2);

			var result = db.Query<ByteTestObj> ("select * from ByteTestObj order by Id").ToList ();
			Assert.AreEqual (2, result.Count);
			Assert.AreEqual (obj1.Value, result[0].Value);
			Assert.AreEqual (obj2.Value, result[1].Value);

			Assert.AreEqual (obj1.Id, result[0].Id);
			Assert.AreEqual (obj2.Id, result[1].Id);

			db.Close ();
		}
	}
}
