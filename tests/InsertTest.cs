﻿
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLite;

using NUnit.Framework;

using System.Diagnostics;

namespace SQLite.Tests
{
    [TestFixture]
    public class InsertTest
    {
        private SQLiteConnection _db;

        public class TestObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }
            public String Text { get; set; }

            public override string ToString ()
            {
            	return string.Format("[TestObj: Id={0}, Text={1}]", Id, Text);
            }

        }

        public class TestObj2
        {
            [PrimaryKey]
            public int Id { get; set; }
            public String Text { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Text={1}]", Id, Text);
            }

        }

        public class OneColumnObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }
        }

		public class UniqueObj
		{
			[PrimaryKey]
			public int Id { get; set; }
		}

        private SQLiteConnection GetConnection() {
            var db = TestDb.GetMemoryDb();
            var response = db.CreateTable<TestObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            response = db.CreateTable<TestObj2>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            response = db.CreateTable<OneColumnObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            response = db.CreateTable<UniqueObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            return db;
        }

        [SetUp]
        public void Setup()
        {
            _db = GetConnection();
        }

        [TearDown]
        public void TearDown()
        {
            if (_db != null) _db.Close();
        }

        [Test]
        public void InsertALot()
        {
			int n = 10000;
			var objs = Enumerable.Range(1, n).Select(x => new TestObj() {
				Text = "I am"
			}).ToList();

			_db.Trace = false;

			var sw = new Stopwatch();
			sw.Start();

			var numIn = _db.InsertAll(objs);

			sw.Stop();

			Assert.AreEqual(n, numIn, "Num inserted must = num objects");

			var numCount = _db.ExecuteScalar<int>("select count(*) from TestObj");

			Assert.AreEqual(numCount, n, "Num counted must = num objects");

			var inObjs = _db.Query<TestObj>("select * from TestObj").ToList();

			for (var i = 0; i < inObjs.Count; i++) {
				Assert.AreEqual(i+1, inObjs[i].Id);
				Assert.AreEqual("I am", inObjs[i].Text);
			}
        }

		[Test]
		public void InsertTraces ()
		{
			var oldTracer = _db.Tracer;
			var oldTrace = _db.Trace;

			var traces = new List<string> ();
			_db.Tracer = traces.Add;
			_db.Trace = true;

			var obj1 = new TestObj () { Text = "GLaDOS loves tracing!" };
			var numIn1 = _db.Insert (obj1);

			Assert.AreEqual (1, numIn1);
			Assert.AreEqual (1, traces.Count);

			_db.Tracer = oldTracer;
			_db.Trace = oldTrace;
		}

        [Test]
        public void InsertTwoTimes()
        {
            var obj1 = new TestObj() { Text = "GLaDOS loves testing!" };
            var obj2 = new TestObj() { Text = "Keep testing, just keep testing" };


            var numIn1 = _db.Insert(obj1);
            var numIn2 = _db.Insert(obj2);
            Assert.AreEqual(1, numIn1);
            Assert.AreEqual(1, numIn2);

            var result = _db.Query<TestObj>("select * from TestObj").ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(obj1.Text, result[0].Text);
            Assert.AreEqual(obj2.Text, result[1].Text);
        }

        [Test]
        public void InsertIntoTwoTables()
        {
            var obj1 = new TestObj() { Text = "GLaDOS loves testing!" };
            var obj2 = new TestObj2() { Text = "Keep testing, just keep testing" };

            var numIn1 = _db.Insert(obj1);
            Assert.AreEqual(1, numIn1);
            var numIn2 = _db.Insert(obj2);
            Assert.AreEqual(1, numIn2);

            var result1 = _db.Query<TestObj>("select * from TestObj").ToList();
            Assert.AreEqual(numIn1, result1.Count);
            Assert.AreEqual(obj1.Text, result1.First().Text);

            var result2 = _db.Query<TestObj>("select * from TestObj2").ToList();
            Assert.AreEqual(numIn2, result2.Count);
        }

        [Test]
        public void InsertWithExtra()
        {
            var obj1 = new TestObj2() { Id=1, Text = "GLaDOS loves testing!" };
            var obj2 = new TestObj2() { Id=1, Text = "Keep testing, just keep testing" };
            var obj3 = new TestObj2() { Id=1, Text = "Done testing" };

            _db.Insert(obj1);


            try {
                _db.Insert(obj2);
                Assert.Fail("Expected unique constraint violation");
            }
            catch (SQLiteException) {
            }
            _db.Insert(obj2, "OR REPLACE");


            try {
                _db.Insert(obj3);
                Assert.Fail("Expected unique constraint violation");
            }
            catch (SQLiteException) {
            }
            _db.Insert(obj3, "OR IGNORE");

            var result = _db.Query<TestObj>("select * from TestObj2").ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(obj2.Text, result.First().Text);
        }

        [Test]
        public void InsertIntoOneColumnAutoIncrementTable()
        {
            var obj = new OneColumnObj();
            _db.Insert(obj);

            var result = _db.Get<OneColumnObj>(1);
            Assert.AreEqual(1, result.Id);
        }

		[Test]
		public void InsertAllSuccessOutsideTransaction()
		{
			var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();

			_db.InsertAll(testObjects);

			Assert.AreEqual(testObjects.Count, _db.Table<UniqueObj>().Count());
		}

		[Test]
		public void InsertAllFailureOutsideTransaction()
		{
			var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();
			testObjects[testObjects.Count - 1].Id = 1; // causes the insert to fail because of duplicate key

			ExceptionAssert.Throws<SQLiteException>(() => _db.InsertAll(testObjects));

			Assert.AreEqual(0, _db.Table<UniqueObj>().Count());
		}

		[Test]
		public void InsertAllSuccessInsideTransaction()
		{
			var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();

			_db.RunInTransaction(() => {
				_db.InsertAll(testObjects);
			});

			Assert.AreEqual(testObjects.Count, _db.Table<UniqueObj>().Count());
		}

		[Test]
		public void InsertAllFailureInsideTransaction()
		{
			var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();
			testObjects[testObjects.Count - 1].Id = 1; // causes the insert to fail because of duplicate key

			ExceptionAssert.Throws<SQLiteException>(() => _db.RunInTransaction(() => {
				_db.InsertAll(testObjects);
			}));

			Assert.AreEqual(0, _db.Table<UniqueObj>().Count());
		}

		[Test]
		public void InsertOrReplace ()
		{
			_db.Trace = true;
			_db.InsertAll (from i in Enumerable.Range(1, 20) select new TestObj { Text = "#" + i });

			Assert.AreEqual (20, _db.Table<TestObj> ().Count ());

			var t = new TestObj { Id = 5, Text = "Foo", };
			_db.InsertOrReplace (t);

			var r = (from x in _db.Table<TestObj> () orderby x.Id select x).ToList ();
			Assert.AreEqual (20, r.Count);
			Assert.AreEqual ("Foo", r[4].Text);
		}
    }
}
