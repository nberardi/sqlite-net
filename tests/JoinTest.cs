using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using SQLite;

using NUnit.Framework;

using System.Diagnostics;

namespace SQLite.Tests
{    
    [TestFixture]
    public class JoinTest
    {
		SQLiteConnection _db;
		
		[SetUp]
		public void SetUp ()
		{
			_db = TestDb.GetMemoryDb();
			_db.CreateTable<Product> ();
			_db.CreateTable<Order> ();
			_db.CreateTable<OrderLine> ();
			
			var p1 = new Product { Name = "One", };
			var p2 = new Product { Name = "Two", };
			var p3 = new Product { Name = "Three", };
			_db.InsertAll (new [] { p1, p2, p3 } );
			
			var o1 = new Order { PlacedTime = DateTime.Now, };
			var o2 = new Order { PlacedTime = DateTime.Now, };
			_db.InsertAll (new [] { o1, o2 } );
			
			_db.InsertAll (new [] {
				new OrderLine {
					OrderId = o1.Id,
					ProductId = p1.Id,
					Quantity = 1,
				},
				new OrderLine {
					OrderId = o1.Id,
					ProductId = p2.Id,
					Quantity = 2,
				},
				new OrderLine {
					OrderId = o2.Id,
					ProductId = p3.Id,
					Quantity = 3,
				},
			});
		}
		
		class R 
		{
		}
		
		//[Test]
		//public void JoinThenWhere ()
		//{
		//	var q = from ol in _db.Table<OrderLine> ()
		//			join o in _db.Table<Order> () on ol.OrderId equals o.Id
		//			where o.Id == 1
		//			select new { o.Id, ol.ProductId, ol.Quantity };
			
		//	var r = System.Linq.Enumerable.ToList (q);
			
		//	Assert.AreEqual (2, r.Count);
		//}
		
		//[Test]
		//public void WhereThenJoin ()
		//{
		//	var q = from ol in _db.Table<OrderLine> ()
		//			where ol.OrderId == 1
		//			join o in _db.Table<Order> () on ol.OrderId equals o.Id
		//			select new { o.Id, ol.ProductId, ol.Quantity };
			
		//	var r = System.Linq.Enumerable.ToList (q);
			
		//	Assert.AreEqual (2, r.Count);
		//}
	}
}
