using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using SQLite;

using NUnit.Framework;

namespace SQLite.Tests
{

	[TestFixture]
	public class NotNullAttributeTest
	{
		private class NotNullNoPK
		{
			[PrimaryKey, AutoIncrement]
			public int? objectId { get; set; }
			[NotNull]
			public int? RequiredIntProp { get; set; }
			public int? OptionalIntProp { get; set; }
			[NotNull]
			public string RequiredStringProp { get; set; }
			public string OptionalStringProp { get; set; }
			[NotNull]
			public string AnotherRequiredStringProp { get; set; }
		}

		private class ClassWithPK
		{
			[PrimaryKey, AutoIncrement]
			public int Id { get; set; }
		}

		private IEnumerable<SQLiteColumn> GetExpectedColumnInfos (Type type)
		{
			var expectedValues = from prop in type.GetProperties (BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetProperty)
								 select new SQLiteColumn {
									 Name = prop.Name,
									 IsNotNull = !((prop.GetCustomAttributes (typeof (NotNullAttribute), true).Count () == 0) && (prop.GetCustomAttributes (typeof (PrimaryKeyAttribute), true).Count () == 0))
								 };

			return expectedValues;
		}

		[Test]
		public void PrimaryKeyHasNotNullConstraint ()
		{
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<ClassWithPK> ();
				var cols = db.GetTableInfo ("ClassWithPK");

				var joined = from expected in GetExpectedColumnInfos (typeof (ClassWithPK))
							 join actual in cols on expected.Name equals actual.Name
							 where actual.IsNotNull != expected.IsNotNull
							 select actual.Name;

				Assert.AreNotEqual (0, cols.Count (), "Failed to get table info");
				Assert.IsTrue (joined.Count () == 0, string.Format ("not null constraint was not created for the following properties: {0}"
					, string.Join (", ", joined.ToArray ())));
			}
		}

		[Test]
		public void CreateTableWithNotNullConstraints ()
		{
			using (var db = TestDb.GetMemoryDb()) {
				db.CreateTable<NotNullNoPK> ();
				var cols = db.GetTableInfo ("NotNullNoPK");

				var joined = from expected in GetExpectedColumnInfos (typeof (NotNullNoPK))
							 join actual in cols on expected.Name equals actual.Name
							 where actual.IsNotNull != expected.IsNotNull
							 select actual.Name;

				Assert.AreNotEqual (0, cols.Count (), "Failed to get table info");
				Assert.IsTrue (joined.Count () == 0, string.Format ("not null constraint was not created for the following properties: {0}"
					, string.Join (", ", joined.ToArray ())));
			}
		}

		[Test]
		public void InsertWithNullsThrowsException ()
		{
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<NotNullNoPK> ();

				try {
					NotNullNoPK obj = new NotNullNoPK ();
					db.Insert (obj);
				}
				catch (NotNullConstraintViolationException) {
					return;
				}
				catch (SQLiteException ex) {
					if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
						Inconclusive ();
						return;
					}
				}
			}
			Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
		}


		[Test]
		public void UpdateWithNullThrowsException ()
		{
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<NotNullNoPK> ();

				try {
					NotNullNoPK obj = new NotNullNoPK () {
						AnotherRequiredStringProp = "Another required string",
						RequiredIntProp = 123,
						RequiredStringProp = "Required string"
					};
					db.Insert (obj);
					obj.RequiredStringProp = null;
					db.Update (obj);
				}
				catch (NotNullConstraintViolationException) {
					return;
				}
				catch (SQLiteException ex) {
					if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
						Inconclusive ();
						return;
					}
				}
			}
			Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
		}

		[Test]
		public void NotNullConstraintExceptionListsOffendingColumnsOnInsert ()
		{
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<NotNullNoPK> ();

				try {
					NotNullNoPK obj = new NotNullNoPK () { RequiredStringProp = "Some value" };
					db.Insert (obj);
				}
				catch (NotNullConstraintViolationException ex) {
					string expected = "RequiredIntProp";
                    string actual = ex.Column?.PropertyName;

					Assert.AreEqual (expected, actual, "NotNullConstraintViolationException did not correctly list the columns that violated the constraint");
					return;
				}
				catch (SQLiteException ex) {
					if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
						Inconclusive ();
						return;
					}
				}
				Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
			}
		}

		[Test]
		public void NotNullConstraintExceptionListsOffendingColumnsOnUpdate ()
		{
			// Skip this test if the Dll doesn't support the extended SQLITE_CONSTRAINT codes

				using (SQLiteConnection db = TestDb.GetMemoryDb()) {
					db.CreateTable<NotNullNoPK> ();

					try {
						NotNullNoPK obj = new NotNullNoPK () {
							AnotherRequiredStringProp = "Another required string",
							RequiredIntProp = 123,
							RequiredStringProp = "Required string"
						};
						db.Insert (obj);
						obj.RequiredStringProp = null;
						db.Update (obj);
					}
					catch (NotNullConstraintViolationException ex) {
						string expected = "RequiredStringProp";
                        string actual = ex.Column?.PropertyName;

						Assert.AreEqual (expected, actual, "NotNullConstraintViolationException did not correctly list the columns that violated the constraint");

						return;
					}
					catch (SQLiteException ex) {
						if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
							Inconclusive ();
							return;
						}
					}
					catch (Exception ex) {
						Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. An exception of type {0} was thrown instead.", ex.GetType ().Name);
					}
				Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
			}
		}

		[Test]
		public void InsertQueryWithNullThrowsException ()
		{
			// Skip this test if the Dll doesn't support the extended SQLITE_CONSTRAINT codes
			if (SQLite3.LibraryVersionInt32 >= 3007017) {
				using (SQLiteConnection db = TestDb.GetMemoryDb()) {

					db.CreateTable<NotNullNoPK> ();

					try {
						db.Execute ("insert into \"NotNullNoPK\" (AnotherRequiredStringProp, RequiredIntProp, RequiredStringProp) values(?, ?, ?)",
							new object[] { "Another required string", 123, null });
					}
					catch (NotNullConstraintViolationException) {
						return;
					}
					catch (SQLiteException ex) {
						if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
							Inconclusive ();
							return;
						}
					}
					catch (Exception ex) {
						Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. An exception of type {0} was thrown instead.", ex.GetType ().Name);
					}
				}
				Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
			}
		}

		[Test]
		public void UpdateQueryWithNullThrowsException ()
		{
			// Skip this test if the Dll doesn't support the extended SQLITE_CONSTRAINT codes
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<NotNullNoPK> ();

				try {
					db.Execute ("insert into \"NotNullNoPK\" (AnotherRequiredStringProp, RequiredIntProp, RequiredStringProp) values(?, ?, ?)",
						new object[] { "Another required string", 123, "Required string" });

					db.Execute ("update \"NotNullNoPK\" set AnotherRequiredStringProp=?, RequiredIntProp=?, RequiredStringProp=? where ObjectId=?",
						new object[] { "Another required string", 123, null, 1 });
				}
				catch (NotNullConstraintViolationException) {
					return;
				}
				catch (SQLiteException ex) {
					if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
						Inconclusive ();
						return;
					}
				}
				catch (Exception ex) {
					Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. An exception of type {0} was thrown instead.", ex.GetType ().Name);
				}
				Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
			}
		}

		[Test]
		public void ExecuteNonQueryWithNullThrowsException ()
		{
			using (SQLiteConnection db = TestDb.GetMemoryDb()) {

				db.CreateTable<NotNullNoPK> ();

				try {
					NotNullNoPK obj = new NotNullNoPK () {
						AnotherRequiredStringProp = "Another required prop",
						RequiredIntProp = 123,
						RequiredStringProp = "Required string prop"
					};
					db.Insert (obj);

					NotNullNoPK obj2 = new NotNullNoPK () {
						objectId = 1,
						OptionalIntProp = 123,
					};
					db.InsertOrReplace (obj2);
				}
				catch (NotNullConstraintViolationException) {
					return;
				}
				catch (SQLiteException ex) {
					if (SQLite3.LibraryVersionInt32 < 3007017 && ex.Result == Result.Constraint) {
						Inconclusive ();
						return;
					}
				}
				catch (Exception ex) {
					Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. An exception of type {0} was thrown instead.", ex.GetType ().Name);
				}
			}
			Assert.Fail ("Expected an exception of type NotNullConstraintViolationException to be thrown. No exception was thrown.");
		}

		void Inconclusive ()
		{
#if !NETFX_CORE
			Console.WriteLine ("Detailed constraint information is only available in SQLite3 version 3.7.17 and above.");
#endif
		}

	}
}
