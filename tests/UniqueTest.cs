using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

namespace SQLite.Tests
{
	[TestFixture]
	public class UniqueIndexTest
	{
		public class TheOne {
			[PrimaryKey, AutoIncrement]
			public int ID { get; set; }

			[Unique (Name = "UX_Uno")]
			public int Uno { get; set;}

			[Unique (Name = "UX_Dos")]
			public int Dos { get; set;}

			[Unique (Name = "UX_Dos")]
			public int Tres { get; set;}

			[Indexed (Name = "UX_Uno_bool", Unique = true)]
			public int Cuatro { get; set;}

			[Indexed (Name = "UX_Dos_bool", Unique = true)]
			public int Cinco { get; set;}
			[Indexed (Name = "UX_Dos_bool", Unique = true)]
			public int Seis { get; set;}
		}

		public class IndexColumns {
			public int seqno { get; set;}
			public int cid { get; set;}
			public string name { get; set; }
		}

		public class IndexInfo {
			public int seq { get; set;}
			public string name { get; set;}
			public bool unique { get; set;}
		}

        private SQLiteConnection GetConnection() {
            var db = TestDb.GetMemoryDb();
            var response = db.CreateTable<TheOne>();
            Assert.That(response, Is.EqualTo(CreateTableResult.Created));
            return db;
        }

        public static object[] CheckIndexCases =
        {
            new object[] { "UX_Uno", true, new string[] { "Uno" } },
            new object[] { "UX_Dos", true, new string[] { "Dos", "Tres"} },
            new object[] { "UX_Uno_bool", true, new string[] { "Cuatro" } },
            new object[] { "UX_Dos_bool", true, new string[] { "Cinco", "Seis" } }
        };

        [TestCaseSource("CheckIndexCases")]
		public void CheckIndex (string iname, bool unique, string [] columns)
		{
			if (columns == null)
				Assert.Fail("Columns are required for the test to succeed.");

            using (var db = GetConnection()) {

				var indexes = db.Query<IndexInfo> ($"PRAGMA INDEX_LIST (\"{nameof(TheOne)}\")");
				Assert.That (indexes.Count, Is.EqualTo(4), "Wrong number of indexes created for table.");

                var idx = indexes.SingleOrDefault (i => i.name == iname);

                Assert.IsNotNull (idx, String.Format ("Index {0} not found", iname));
                Assert.AreEqual (idx.unique, unique, String.Format ("Index {0} unique expected {1} but got {2}", iname, unique, idx.unique));

                var idx_columns = db.Query<IndexColumns> (String.Format ("PRAGMA INDEX_INFO (\"{0}\")", iname));
                Assert.AreEqual (columns.Length, idx_columns.Count, String.Format ("# of columns: expected {0}, got {1}", columns.Length, idx_columns.Count));

                foreach (var col in columns)
                    Assert.That(idx_columns.Any(c => c.name == col), $"Column {col} not in index {idx.name}.");
            }
		}
	}
}
