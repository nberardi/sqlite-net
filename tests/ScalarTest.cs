using System;
using System.Linq;

using NUnit.Framework;

namespace SQLite.Tests
{
	[TestFixture]
	public class ScalarTest
	{
		class TestTable
		{
			[PrimaryKey, AutoIncrement]
			public int Id { get; set; }
			public int Two { get; set; }
		}

		const int Count = 100;

		SQLiteConnection CreateDb ()
		{
			var db = TestDb.GetMemoryDb();
			db.CreateTable<TestTable> ();
			var items = from i in Enumerable.Range (0, Count)
				select new TestTable { Two = 2 };
			db.InsertAll (items);
			Assert.AreEqual (Count, db.Table<TestTable> ().Count ());
			return db;
		}


		[Test]
		public void Int32 ()
		{
			var db = CreateDb ();
			
			var r = db.ExecuteScalar<int> ("SELECT SUM(Two) FROM TestTable");

			Assert.AreEqual (Count * 2, r);
		}

		[Test]
		public void SelectSingleRowValue ()
		{
			var db = CreateDb ();

			var r = db.ExecuteScalar<int> ("SELECT Two FROM TestTable WHERE Id = 1 LIMIT 1");

			Assert.AreEqual (2, r);
		}
	}
}

