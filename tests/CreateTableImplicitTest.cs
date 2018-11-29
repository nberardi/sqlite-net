using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using NUnit.Framework;

namespace SQLite.Tests
{
    [TestFixture]
    public class CreateTableImplicitTest
    {
        private TestDb _db;

        [OneTimeSetUp]
        public void Init()
        {
            _db = new TestDb(":memory:");
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            _db.Dispose();
        }

        [SetUp]
        public void SetUp()
        {
            _db.DropTable<NoAttributes>();
            _db.DropTable<PkAttribute>();
            TableMapping.Clear();
        }

        class NoAttributes
        {
            public int Id { get; set; }
            public string AColumn { get; set; }
            public int IndexedId { get; set; }
        }

        class PkAttribute
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string AColumn { get; set; }
            public int IndexedId { get; set; }
        }

        private void CheckPK(TestDb db)
        {
            for (int i = 1; i <= 10; i++)
            {
                var na = new NoAttributes { Id = i, AColumn = i.ToString(), IndexedId = 0 };
                db.Insert(na);
            }
            var item = db.Get<NoAttributes>(2);
            Assert.IsNotNull(item);
            Assert.AreEqual(2, item.Id);
        }

		[Test]
        public void WithoutImplicitMapping ()
        {
            _db.CreateTable<NoAttributes>();

            var mapping = _db.GetMapping<NoAttributes>();

            Assert.IsNull (mapping.PK, "Mapping primary key should be null");

            var column = mapping.Columns[2];
            Assert.AreEqual("IndexedId", column.Name, "IndexedId is the 3 column");
            Assert.IsFalse(column.Indices.Any(), "Table shouldn't have any indexes");

            Assert.That(() => CheckPK(_db), Throws.TypeOf<AssertionException>(), "The primary key check should fail");
        }

        [Test]
        public void ImplicitPK()
        {
            _db.CreateTable<NoAttributes>(CreateFlags.ImplicitPK);

            var mapping = _db.GetMapping<NoAttributes>();

            Assert.IsNotNull(mapping.PK, "Mapping primary key should not be null");
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsFalse(mapping.PK.IsAutoInc);

            CheckPK(_db);
        }


        [Test]
        public void ImplicitAutoInc()
        {
            _db.CreateTable<PkAttribute>(CreateFlags.AutoIncPK);

            var mapping = _db.GetMapping<PkAttribute>();

            Assert.IsNotNull(mapping.PK);
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsTrue(mapping.PK.IsAutoInc);
        }

        [Test]
        public void ImplicitIndex()
        {
            _db.CreateTable<NoAttributes>(CreateFlags.ImplicitIndex);

            var mapping = _db.GetMapping<NoAttributes>();
            var column = mapping.Columns[2];
            Assert.AreEqual("IndexedId", column.Name);
            Assert.IsTrue(column.Indices.Any());
        }

        [Test]
        public void ImplicitPKAutoInc()
        {
            _db.CreateTable(typeof(NoAttributes), CreateFlags.ImplicitPK | CreateFlags.AutoIncPK);

            var mapping = _db.GetMapping<NoAttributes>();

            Assert.IsNotNull(mapping.PK);
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsTrue(mapping.PK.IsAutoInc);
        }

        [Test]
        public void ImplicitAutoIncAsPassedInTypes()
        {
            _db.CreateTable(typeof(PkAttribute), CreateFlags.AutoIncPK);

            var mapping = _db.GetMapping<PkAttribute>();

            Assert.IsNotNull(mapping.PK);
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsTrue(mapping.PK.IsAutoInc);
        }

        [Test]
        public void ImplicitPkAsPassedInTypes()
        {
            _db.CreateTable(typeof(NoAttributes), CreateFlags.ImplicitPK);

            var mapping = _db.GetMapping<NoAttributes>();

            Assert.IsNotNull(mapping.PK);
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsFalse(mapping.PK.IsAutoInc);
        }

        [Test]
        public void ImplicitPKAutoIncAsPassedInTypes()
        {
            _db.CreateTable(typeof(NoAttributes), CreateFlags.ImplicitPK | CreateFlags.AutoIncPK);

            var mapping = _db.GetMapping<NoAttributes>();

            Assert.IsNotNull(mapping.PK);
            Assert.AreEqual("Id", mapping.PK.Name);
            Assert.IsTrue(mapping.PK.IsPK);
            Assert.IsTrue(mapping.PK.IsAutoInc);
        }
    }
}

