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
using System.Linq;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    public class SQLiteException : Exception
    {
		public TableColumn Column { get; private set; }

		public Result Result { get; }
        public ExtendedResult ExtendedResult { get; }

        public string SQLQuery { get; }

        public SQLiteException(Result r, string message, string sql = null)
            : this(r, (ExtendedResult)r, message, sql) { }

        protected SQLiteException(Result r, ExtendedResult er, string message, string sql = null)
            : base(message)
        {
            // this mathmatical trimming will continue to work as long as the result values stay below 255
            Result = (Result)((int)r & 0xff);
            ExtendedResult = er;
            SQLQuery = sql;
        }

		internal void PopulateColumnFromTableMapping(TableMapping mapping)
		{
			// extract column for which unique constraint was violated
			var message = Message.ToLowerInvariant();
			Column = mapping.Columns.Where(x => x.IsAutoInc == false && message.Contains(x.Name.ToLowerInvariant())).FirstOrDefault();
		}

		public override string ToString()
        {
            if (String.IsNullOrWhiteSpace(SQLQuery) == false)
                return $"--------------------------\n{SQLQuery}\n--------------------------\n{base.ToString()}";

            return base.ToString();
        }
    }

    public class NotNullConstraintViolationException : SQLiteException
    {
		public NotNullConstraintViolationException(Result r, string message, string sql = null)
			: base(r, message, sql) { }
	}

    public class UniqueConstraintViolationException : SQLiteException
    {
		public UniqueConstraintViolationException(Result r, string message, string sql = null)
            : base(r, message, sql) { }
    }
}
