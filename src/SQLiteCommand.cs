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
using System.Diagnostics;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    /// <summary>
    /// Since the insert or update never changed, we only need to prepare once.
    /// </summary>
    public class SQLiteCommand : IDisposable, IEquatable<SQLiteCommand>
    {
        private readonly static IntPtr NegativePointer = new IntPtr(-1);

        private const string DateTimeExactStoreFormat = "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff";

        private bool _disposed;

        public bool Prepared { get; private set; }

        protected SQLiteConnection Connection { get; private set; }

        public string CommandText { get; set; }

        protected Sqlite3Statement Statement { get; set; }
        internal static readonly Sqlite3Statement NullStatement = default(Sqlite3Statement);

        public SQLiteCommand(SQLiteConnection conn)
        {
            Connection = conn;
        }

        public SQLiteCommand(SQLiteConnection conn, string commandText)
        {
            Connection = conn;
            CommandText = commandText;
        }

        protected virtual void OnExecutionStarted() { }

        protected virtual void OnExecutionEnded() { }

        /// <summary>
        /// Invoked every time an instance is loaded from the database.
        /// </summary>
        /// <param name='obj'>
        /// The newly created object.
        /// </param>
        /// <remarks>
        /// This can be overridden in combination with the <see cref="SQLiteConnection.NewCommand"/>
        /// method to hook into the life-cycle of objects.
        ///
        /// Type safety is not possible because MonoTouch does not support virtual generic methods.
        /// </remarks>
        protected virtual void OnInstanceCreated(object obj)
        {
            // Can be overridden.
        }

        public IEnumerable<T> ExecuteQuery<T>()
        {
            return ExecuteQuery<T>(Connection.GetMapping<T>(), null);
        }

        public IEnumerable<T> ExecuteQuery<T>(object[] source)
        {
            return ExecuteQuery<T>(Connection.GetMapping<T>(), source);
        }

        public IEnumerable<T> ExecuteQuery<T>(TableMapping map, object[] source)
        {
            CheckDisposed();
            Log(nameof(ExecuteQuery), source);
            OnExecutionStarted();

            var sw = Stopwatch.StartNew();
            try
            {
                var r = Result.OK;

                if (!Prepared)
                {
                    Statement = Prepare();
                    Prepared = true;
                }

                //bind the values.
                if (source != null)
                    for (int i = 0; i < source.Length; i++)
                        BindParameter(Statement, i + 1, source[i], Connection.StoreDateTimeAsTicks);

                var cols = new TableColumn[SQLite3.ColumnCount(Statement)];

                for (int i = 0; i < cols.Length; i++)
                {
                    var name = SQLite3.ColumnNameUTF8(Statement, i);
                    cols[i] = map.FindColumn(name);
                }

                while (SQLite3.Step(Statement) == Result.Row)
                {
                    var obj = map.CreateInstance();
                    for (int i = 0; i < cols.Length; i++)
                    {
                        if (cols[i] == null)
                            continue;
                        var colType = SQLite3.ColumnType(Statement, i);
                        var val = ReadCol(Statement, i, colType, cols[i].ColumnType, cols[i].PropertyDefaultValue);
                        cols[i].SetValue(obj, val);
                    }
                    OnInstanceCreated(obj);
                    yield return (T) obj;
                }

                if (r == Result.Done || r == Result.OK) { }
                else
                {
                    var msg = SQLite3.GetErrorMessageUTF8(Connection.Handle);
                    var ex = new SQLiteException(r, msg, sql: CommandText);
					ex.PopulateColumnFromTableMapping(map);

					throw ex;
                }
            }
            finally
            {
                if (Statement != null)
                    SQLite3.Reset(Statement);

                sw.Stop();
                Log(sw);
                OnExecutionEnded();
            }
        }

        public T ExecuteScalar<T>()
        {
            return ExecuteScalar<T>(null);
        }

        public T ExecuteScalar<T>(object[] source)
        {
            CheckDisposed();
            Log(nameof(ExecuteScalar), source);
            OnExecutionStarted();

            var sw = Stopwatch.StartNew();
            T val = default(T);

            try
            {
                var r = InternalExecute(source);

                if (r == Result.Row)
                {
                    var colType = SQLite3.ColumnType(Statement, 0);
                    val = ReadCol<T>(Statement, 0, colType);
                }
                else if (r == Result.Done || r == Result.OK) { }
            }
            finally
            {
                if (Statement != null)
                    SQLite3.Reset(Statement);

                sw.Stop();
                Log(sw);
                OnExecutionEnded();
            }

            return val;
        }

        public int ExecuteNonQuery()
        {
            return ExecuteNonQuery(null);
        }

        public int ExecuteNonQuery(object[] source)
        {
            CheckDisposed();
            Log(nameof(ExecuteNonQuery), source);
            OnExecutionStarted();

            var sw = Stopwatch.StartNew();
            try
            {
                var r = InternalExecute(source);

                if (r == Result.Done || r == Result.OK || r == Result.Row)
                {
                    int rowsAffected = SQLite3.Changes(Connection.Handle);
                    return rowsAffected;
                }

                return 0;
            }
            finally
            {
                if (Statement != null)
                    SQLite3.Reset(Statement);

                sw.Stop();
                Log(sw);
                OnExecutionEnded();
            }
        }

        public Result Execute()
        {
            return Execute(null);
        }

        public Result Execute(object[] source)
        {
            CheckDisposed();
            Log(nameof(Execute), source);
            OnExecutionStarted();

            var sw = Stopwatch.StartNew();
            try
            {
                return InternalExecute(source);
            }
            finally
            {
                if (Statement != null)
                    SQLite3.Reset(Statement);

                sw.Stop();
                Log(sw);
                OnExecutionEnded();
            }
        }

        private Result InternalExecute(object[] source)
        {
            var r = Result.OK;

            if (!Prepared)
            {
                Statement = Prepare();
                Prepared = true;
            }

            //bind the values.
            if (source != null)
                for (int i = 0; i < source.Length; i++)
                    BindParameter(Statement, i + 1, source[i], Connection.StoreDateTimeAsTicks);

            r = SQLite3.Step(Statement);

            if (r == Result.Done || r == Result.OK || r == Result.Row)
            {
                return r;
            }
            else
            {
                var msg = SQLite3.GetErrorMessageUTF8(Connection.Handle);

                if ((ExtendedResult)r == ExtendedResult.ConstraintNotNull)
					throw new NotNullConstraintViolationException(r, msg, sql: CommandText);

                if ((ExtendedResult)r == ExtendedResult.ConstraintUnique)
                    throw new UniqueConstraintViolationException(r, msg, sql: CommandText);

                throw new SQLiteException(r, msg, sql: CommandText);
            }
        }

        private void Log(Stopwatch sw)
        {
            if (Connection.Trace && sw.Elapsed > Connection.TraceTimeExceeding)
            {
                Connection.Tracer($"Database took {sw.ElapsedMilliseconds} ms to execute: {CommandText}");
            }
        }

        private void Log(string method, object[] source)
        {
            if (Connection.Trace)
            {
                var parts = new List<string>();
                parts.Add(CommandText);

                if (source != null)
                {
                    var index = 0;
                    foreach (var b in source)
                        parts.Add($"  {index++}: {b}");
                }

                Connection.Tracer($"{method}: {String.Join(Environment.NewLine, parts)}");
            }
        }

        protected virtual Sqlite3Statement Prepare()
        {
            var stmt = SQLite3.Prepare2(Connection.Handle, CommandText);
            return stmt;
        }

        private static void BindParameter(Sqlite3Statement stmt, int index, object value, bool storeDateTimeAsTicks)
        {
			if (value == null) {
				SQLite3.BindNull (stmt, index);
			}
			else {
				if (value is Int32) {
					SQLite3.BindInt32 (stmt, index, (int)value);
				}
				else if (value is String) {
					SQLite3.BindStringUTF8 (stmt, index, (string)value, -1, NegativePointer);
				}
				else if (value is Byte || value is UInt16 || value is SByte || value is Int16) {
					SQLite3.BindInt32 (stmt, index, Convert.ToInt32 (value));
				}
				else if (value is Boolean) {
					SQLite3.BindInt32 (stmt, index, (bool)value ? 1 : 0);
				}
				else if (value is UInt32 || value is Int64) {
					SQLite3.BindInt64 (stmt, index, Convert.ToInt64 (value));
				}
				else if (value is Single || value is Double || value is Decimal) {
					SQLite3.BindDouble (stmt, index, Convert.ToDouble (value));
				}
				else if (value is TimeSpan) {
					SQLite3.BindInt64 (stmt, index, ((TimeSpan)value).Ticks);
				}
				else if (value is DateTime) {
					if (storeDateTimeAsTicks) {
						SQLite3.BindInt64 (stmt, index, ((DateTime)value).Ticks);
					}
					else {
						SQLite3.BindStringUTF8 (stmt, index, ((DateTime)value).ToString (DateTimeExactStoreFormat, System.Globalization.CultureInfo.InvariantCulture), -1, NegativePointer);
					}
				}
				else if (value is DateTimeOffset) {
					SQLite3.BindInt64 (stmt, index, ((DateTimeOffset)value).UtcTicks);
				}
				else if (value is byte[]) {
					SQLite3.BindBlob (stmt, index, (byte[])value, ((byte[])value).Length, NegativePointer);
				}
				else if (value is Guid) {
					SQLite3.BindStringUTF8 (stmt, index, ((Guid)value).ToString (), 72, NegativePointer);
				}
				else if (value is Uri) {
					SQLite3.BindStringUTF8 (stmt, index, ((Uri)value).ToString (), -1, NegativePointer);
				}
				else if (value is StringBuilder) {
					SQLite3.BindStringUTF8 (stmt, index, ((StringBuilder)value).ToString (), -1, NegativePointer);
				}
				else if (value is UriBuilder) {
					SQLite3.BindStringUTF8 (stmt, index, ((UriBuilder)value).ToString (), -1, NegativePointer);
				}
				else {
					// Now we could possibly get an enum, retrieve cached info
					var valueType = value.GetType ();
					var enumInfo = EnumCache.GetInfo (valueType);
					if (enumInfo.IsEnum) {
						var enumIntValue = Convert.ToInt32 (value);
						if (enumInfo.StoreAsText)
							SQLite3.BindStringUTF8 (stmt, index, enumInfo.GetEnumFromInt32Value(enumIntValue).ToString(), -1, NegativePointer);
						else
							SQLite3.BindInt32 (stmt, index, enumIntValue);
					}
					else {
                        var type = value.GetType();
						throw new NotSupportedException ("Cannot store type: " + type);
					}
				}
			}
		}

        private T ReadCol<T>(Sqlite3Statement stmt, int index, ColType type) => (T) (ReadCol(stmt, index, type, typeof(T), default(T)) ?? default(T));

        private object ReadCol(Sqlite3Statement stmt, int index, ColType type, Type clrType, object defaultValue)
		{
			if (type == ColType.Null) {
                return defaultValue;
            }
			else {
				var clrTypeInfo = clrType.GetTypeInfo ();
				if (clrType == typeof (String)) {
					return SQLite3.ColumnStringUTF8 (stmt, index);
				}
				else if (clrType == typeof (Int32)) {
					return (int)SQLite3.ColumnInt32 (stmt, index);
				}
				else if (clrType == typeof (Boolean)) {
					return SQLite3.ColumnInt32 (stmt, index) == 1;
				}
				else if (clrType == typeof (double)) {
					return SQLite3.ColumnDouble (stmt, index);
				}
				else if (clrType == typeof (float)) {
					return (float)SQLite3.ColumnDouble (stmt, index);
				}
				else if (clrType == typeof (TimeSpan)) {
					return new TimeSpan (SQLite3.ColumnInt64 (stmt, index));
				}
				else if (clrType == typeof (DateTime)) {
                    if (Connection.StoreDateTimeAsTicks) {
						return new DateTime (SQLite3.ColumnInt64 (stmt, index));
					}
					else {
						var text = SQLite3.ColumnStringUTF8 (stmt, index);
						DateTime resultDate;
						if (!DateTime.TryParseExact (text, DateTimeExactStoreFormat, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out resultDate)) {
							resultDate = DateTime.Parse (text);
						}
						return resultDate;
					}
				}
				else if (clrType == typeof (DateTimeOffset)) {
                    var value = SQLite3.ColumnInt64(stmt, index);

                    // if the date goes outside the bounds of a valid date time, we need to consider the date as corrupted by
                    // either the disk or some other process, and in that case the date cannot be trusted. With this in mind
                    // we are going to use the default value if the date has been corrupted.
                    if (value < DateTimeOffset.MinValue.Ticks || value > DateTimeOffset.MaxValue.Ticks)
                        return defaultValue;

                    return new DateTimeOffset(value, TimeSpan.Zero);
                }
				else if (clrTypeInfo.IsEnum) {
                    if (type == ColType.Text) {
                        var value = SQLite3.ColumnStringUTF8(stmt, index);
                        return Enum.Parse(clrType, value, true);
                    }
                    else {
                        var value = SQLite3.ColumnInt32(stmt, index);
                        var enumCache = EnumCache.GetInfo(clrType);
                        return enumCache.GetEnumFromInt32Value(value);
                    }
				}
				else if (clrType == typeof (Int64)) {
					return SQLite3.ColumnInt64 (stmt, index);
				}
				else if (clrType == typeof (UInt32)) {
					return (uint)SQLite3.ColumnInt64 (stmt, index);
				}
				else if (clrType == typeof (decimal)) {
					return (decimal)SQLite3.ColumnDouble (stmt, index);
				}
				else if (clrType == typeof (Byte)) {
					return (byte)SQLite3.ColumnInt32 (stmt, index);
				}
				else if (clrType == typeof (UInt16)) {
					return (ushort)SQLite3.ColumnInt32 (stmt, index);
				}
				else if (clrType == typeof (Int16)) {
					return (short)SQLite3.ColumnInt32 (stmt, index);
				}
				else if (clrType == typeof (sbyte)) {
					return (sbyte)SQLite3.ColumnInt32 (stmt, index);
				}
				else if (clrType == typeof (byte[])) {
					return SQLite3.ColumnByteArray (stmt, index);
				}
				else if (clrType == typeof (Guid)) {
					var text = SQLite3.ColumnStringUTF8 (stmt, index);
					return new Guid (text);
				}
                else if (clrType == typeof(Uri)) {
                    var text = SQLite3.ColumnStringUTF8(stmt, index);
                    return new Uri(text);
                }
				else if (clrType == typeof (StringBuilder)) {
					var text = SQLite3.ColumnStringUTF8 (stmt, index);
					return new StringBuilder (text);
				}
				else if (clrType == typeof(UriBuilder)) {
                    var text = SQLite3.ColumnStringUTF8(stmt, index);
                    return new UriBuilder(text);
                }
				else {
					throw new NotSupportedException ("Don't know how to read " + clrType);
				}
			}
		}

        public void ResetStatement()
        {
            try
            {
                SQLite3.Finalize(Statement);
            }
            finally
            {
                Statement = NullStatement;
                Prepared = false;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SQLiteCommand), $"Disposed Query: {CommandText}");
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && Statement != NullStatement)
            {
                _disposed = true;

                ResetStatement();
                Connection = null;
            }
        }

        ~SQLiteCommand()
        {
            Dispose(false);
        }

        public override string ToString() => CommandText;

        public bool Equals(SQLiteCommand other)
        {
            return string.Equals(CommandText, other.CommandText);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SQLiteCommand) obj);
        }

        public override int GetHashCode()
        {
            return (CommandText != null ? CommandText.GetHashCode() : 0);
        }

        public static bool operator ==(SQLiteCommand left, SQLiteCommand right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(SQLiteCommand left, SQLiteCommand right)
        {
            return !Equals(left, right);
        }
    }
}
