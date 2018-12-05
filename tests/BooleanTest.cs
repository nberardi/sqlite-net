using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLite;
using System.Diagnostics;

using NUnit.Framework;

namespace SQLite.Tests
{
    [TestFixture]
    public class BooleanTest
    {
        public class TestObj
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }
            public bool Flag { get; set; }
            public String Text { get; set; }

            public override string ToString()
            {
                return string.Format("TestObj:: ID:{0} Flag:{1} Text:{2}", ID, Flag, Text);
            }
        }

        private SQLiteConnection GetConnection() {
            var db = TestDb.GetMemoryDb();
            var response = db.CreateTable<TestObj>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            return db;
        }

        private int CountWithFlag(SQLiteConnection db, bool flag) => db.ExecuteScalar<int>("select count(*) from TestObj where Flag = ?", flag);
        private List<TestObj> GetList (SQLiteConnection db, bool flag) => db.Query<TestObj>("select * from TestObj where Flag = ?", flag);

        [Test]
        public void TestBoolean()
        {
			var db = GetConnection();

            for (int i = 0; i < 10; i++)
                db.Insert(new TestObj() { Flag = (i % 3 == 0), Text = String.Format("TestObj{0}", i) });

            // count vo which flag is true
            Assert.AreEqual(4, CountWithFlag(db, true));
            Assert.AreEqual(6, CountWithFlag(db, false));

            Debug.WriteLine("TestObj with true flag:");
            foreach (var vo in GetList(db, true))
				Debug.WriteLine (vo.ToString ());

			Debug.WriteLine ("TestObj with false flag:");
            foreach (var vo in GetList(db, false))
				Debug.WriteLine (vo.ToString ());
        }
    }
}
