//
// Copyright (c) 2009-2017 Krueger Systems, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
using System;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
	[AttributeUsage (AttributeTargets.Class)]
    public class TableMapperAttribute : Attribute
    {
        public Type Mapper { get; set; }
        public TableMapperAttribute(Type mapper)
        {
            Mapper = mapper;
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
	public class TableAttribute : Attribute
	{
		public string Name { get; set; }

		/// <summary>
		/// Flag whether to create the table without rowid (see https://sqlite.org/withoutrowid.html)
		///
		/// The default is <c>false</c> so that sqlite adds an implicit <c>rowid</c> to every table created.
		/// </summary>
		public bool WithoutRowId { get; set; }

		public TableAttribute (string name)
		{
			Name = name;
		}
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class ColumnAttribute : Attribute
	{
		public string Name { get; set; }

		public ColumnAttribute (string name)
		{
			Name = name;
		}
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class PrimaryKeyAttribute : Attribute
	{
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class AutoIncrementAttribute : Attribute
	{
	}

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
	public class IndexedAttribute : Attribute
	{
		public string Name { get; set; }
		public int Order { get; set; } = 0;
		public virtual bool Unique { get; set; }
        public TableColumnOrderDirection Direction { get; set; } = TableColumnOrderDirection.NotDefined;

        internal string ColumnName { get; set; }

		public IndexedAttribute () {}

		public IndexedAttribute (string name)
		{
			Name = name;
		}
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class IgnoreAttribute : Attribute
	{
	}

	[AttributeUsage (AttributeTargets.Property)]
    public class DefaultAttribute : Attribute
    {
        /// <summary>
        /// Used to set a default value in database
        /// </summary>
        /// <param name="value">The value to set as default</param>
        public DefaultAttribute(object value)
        {
            Value = value;
        }

        public object Value { get; private set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
	public class UniqueAttribute : IndexedAttribute
	{
		public override bool Unique {
			get { return true; }
			set { /* throw? */ }
		}

        public UniqueAttribute() : base() {}

        public UniqueAttribute(string name) : base(name) {}
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class MaxLengthAttribute : Attribute
	{
		public int Value { get; private set; }

		public MaxLengthAttribute (int length)
		{
			Value = length;
		}
	}

	public sealed class PreserveAttribute : System.Attribute
	{
		public bool AllMembers;
		public bool Conditional;
	}

	/// <summary>
	/// Select the collating sequence to use on a column.
	/// "BINARY", "NOCASE", and "RTRIM" are supported.
	/// "BINARY" is the default.
	/// </summary>
	[AttributeUsage (AttributeTargets.Property)]
	public class CollationAttribute : Attribute
	{
		public string Value { get; private set; }

		public CollationAttribute (string collation)
		{
			Value = collation;
		}
	}

	[AttributeUsage (AttributeTargets.Property)]
	public class NotNullAttribute : Attribute
	{
	}

	[AttributeUsage (AttributeTargets.Enum)]
	public class StoreAsTextAttribute : Attribute
	{
	}
}
