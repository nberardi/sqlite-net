﻿
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLite;

using NUnit.Framework;


namespace SQLite.Tests
{
	[TestFixture]
	public class DropTableTest
	{
		public class Product
		{
			[AutoIncrement, PrimaryKey]
			public int Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
		}



		[Test]
		public void CreateInsertDrop ()
		{
			var db = TestDb.GetMemoryDb();

			db.CreateTable<Product> ();

			db.Insert (new Product {
				Name = "Hello",
				Price = 16,
			});

			var n = db.Table<Product> ().Count ();

			Assert.AreEqual (1, n);

			db.DropTable<Product> ();

			ExceptionAssert.Throws<SQLiteException>(() => db.Table<Product> ().Count ());
		}
	}
}
