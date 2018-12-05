using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

namespace SQLite.Tests
{
	[TestFixture]
	public class SQLCipherTest
	{
		class TestTable
		{
			[PrimaryKey, AutoIncrement]
			public int Id { get; set; }

			public string Value { get; set; }
		}

		[Test]
		public void SetStringKey ()
		{
			string path;

			var key = "SecretPassword";

			using (var db = TestDb.GetFileSystemDb()) {
				path = db.DatabasePath;

				db.SetKey (key);

				db.CreateTable<TestTable> ();
				db.Insert (new TestTable { Value = "Hello" });
			}

			using (var db = new SQLiteConnection (path)) {
				path = db.DatabasePath;

				db.SetKey (key);

				var r = db.Table<TestTable> ().First ();

				Assert.AreEqual ("Hello", r.Value);
			}
		}

		[Test]
		public void SetBytesKey ()
		{
			string path;

			var rand = new Random ();
			var key = new byte[32];
			rand.NextBytes (key);

			using (var db = TestDb.GetFileSystemDb()) {
				path = db.DatabasePath;

				db.SetKey (key);

				db.CreateTable<TestTable> ();
				db.Insert (new TestTable { Value = "Hello" });
			}

			using (var db = new SQLiteConnection (path)) {
				path = db.DatabasePath;

				db.SetKey (key);

				var r = db.Table<TestTable> ().First ();

				Assert.AreEqual ("Hello", r.Value);
			}
		}

		[Test]
		public void SetBadBytesKey ()
		{
			try {
				using (var db = TestDb.GetMemoryDb()) {
					db.SetKey (new byte[] { 1, 2, 3, 4 });
				}
				Assert.Fail ("Should have thrown");
			}
			catch (ArgumentException) {
			}
		}
	}
}
