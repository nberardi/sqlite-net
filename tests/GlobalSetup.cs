using System;
using System.Diagnostics;
using NUnit.Framework;

#pragma warning disable 4014
namespace SQLite.Tests
{
    [SetUpFixture]
    public class Tests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            NativeLibraryLoader.TryLoad("sqlite3");
        }
    }
}
