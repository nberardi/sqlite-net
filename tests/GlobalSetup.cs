using System;
using System.Diagnostics;

#if NETFX_CORE
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SetUp = Microsoft.VisualStudio.TestTools.UnitTesting.TestInitializeAttribute;
using TestFixture = Microsoft.VisualStudio.TestTools.UnitTesting.TestClassAttribute;
using Test = Microsoft.VisualStudio.TestTools.UnitTesting.TestMethodAttribute;
#else
using NUnit.Framework;
#endif

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
