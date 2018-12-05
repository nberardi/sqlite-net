using System;
using NUnit.Framework;

namespace SQLite.Tests
{
	[TestFixture]
	public class DateTimeTest
	{
		class TestObj
		{
			[PrimaryKey, AutoIncrement]
			public int Id { get; set; }

			public string Name { get; set; }
			public DateTime ModifiedTime { get; set; }
		}


		[Test]
		public void AsTicks ()
		{
			var db = TestDb.GetMemoryDb(storeDateTimeAsTicks: true);
			TestDateTime (db);
		}

		[Test]
		public void AsStrings ()
		{
			var db = TestDb.GetMemoryDb(storeDateTimeAsTicks: false);
			TestDateTime (db);
		}

		private void TestDateTime (SQLiteConnection db)
		{
			db.CreateTable<TestObj> ();

			TestObj o, o2;

			//
			// Ticks
			//
			o = new TestObj {
				ModifiedTime = new DateTime (2012, 1, 14, 3, 2, 1, 234),
			};
			db.Insert (o);
			o2 = db.Get<TestObj> (o.Id);
			Assert.AreEqual (o.ModifiedTime, o2.ModifiedTime);
		}
	}
}

