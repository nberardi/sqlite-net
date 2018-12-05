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
using System.Collections;
using System.Diagnostics;
using System.Collections.Generic;
using ConcurrentCommandDictionary = System.Collections.Concurrent.ConcurrentDictionary<string, SQLite.SQLiteCommand>;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    /// <summary>
    /// An open connection to a SQLite database.
    /// </summary>
    [Preserve (AllMembers = true)]
	public partial class SQLiteConnection : IDisposable
	{
		private bool _open;
		private TimeSpan _busyTimeout;
		private Action<string> _tracer;
		private ConcurrentCommandDictionary _cachedPreparedCommands;

		private int _transactionDepth = 0;
		private Random _rand = new Random ();

		public Sqlite3DatabaseHandle Handle { get; private set; }
        public static readonly Sqlite3DatabaseHandle NullHandle = default(Sqlite3DatabaseHandle);

		/// <summary>
		/// Gets the database path used by this connection.
		/// </summary>
		public string DatabasePath { get; private set; }

		/// <summary>
		/// Gets the SQLite library version number. 3007014 would be v3.7.14
		/// </summary>
        [Obsolete("Use LibraryVersion instead.")]
        public int LibVersionNumber => SQLite3.LibraryVersionInt32;

		/// <summary>
		/// Whether Trace lines should be written that show the execution time of queries.
		/// </summary>
        [Obsolete("Use TraceTime instead.")]
        public bool TimeExecution
        {
            get { return TraceTime; }
            set { TraceTime = value; }
        }

		/// <summary>
		/// Whether to writer queries to <see cref="Tracer"/> during execution.
		/// </summary>
		/// <value>The tracer.</value>
		public bool Trace { get; set; }

        /// <summary>
        /// Whether Trace lines should be written that show the execution time of queries.
        /// </summary>
        public bool TraceTime { get; set; } = true;

        /// <summary>
        /// Write a log message when the time exceeds a defined threshold.
        /// </summary>
        public TimeSpan TraceTimeExceeding { get; set; } = TimeSpan.FromMilliseconds(500);

        /// <summary>
		/// The delegate responsible for writing trace lines.
		/// </summary>
		/// <value>The tracer.</value>
		public Action<string> Tracer
		{
			get => _tracer;
			set { _tracer = value ?? (line => Debug.WriteLine(line)); }
		}

		/// <summary>
		/// Whether to store DateTime properties as ticks (true) or strings (false).
		/// </summary>
		public bool StoreDateTimeAsTicks { get; private set; }

		/// <summary>
		/// Gets the SQLite library version number. 3007014 would be v3.7.14
		/// </summary>
	    public SQLiteVersion LibraryVersion => SQLite3.LibraryVersion;

#if !NO_SQLITEPCL_RAW_BATTERIES
		static SQLiteConnection ()
		{
			SQLitePCL.Batteries_V2.Init ();
		}
#endif

		/// <summary>
		/// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
		/// </summary>
		/// <param name="databasePath">
		/// Specifies the path to the database file.
		/// </param>
		/// <param name="openFlags">
		/// Flags controlling how the connection should be opened.
		/// </param>
		/// <param name="storeDateTimeAsTicks">
		/// Specifies whether to store DateTime properties as ticks (true) or strings (false). You
		/// absolutely do want to store them as Ticks in all new projects. The value of false is
		/// only here for backwards compatibility. There is a *significant* speed advantage, with no
		/// down sides, when setting storeDateTimeAsTicks = true.
		/// If you use DateTimeOffset properties, it will be always stored as ticks regardingless
		/// the storeDateTimeAsTicks parameter.
		/// </param>
		/// <exception cref="ArgumentException"></exception>
		public SQLiteConnection (string databasePath, SQLiteOpenFlags openFlags = SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create, bool storeDateTimeAsTicks = true)
		{
			if (databasePath == null)
				throw new ArgumentException ("Must be specified", nameof(databasePath));

			DatabasePath = databasePath;

			Sqlite3DatabaseHandle handle;

			var r = SQLite3.Open (databasePath, out handle, (int)openFlags, IntPtr.Zero);

			Handle = handle;
			if (r != Result.OK) {
				throw new SQLiteException(r, String.Format ("Could not open database file: {0} ({1})", DatabasePath, r));
			}

            // enabled extended result codes
            SQLitePCL.raw.sqlite3_extended_result_codes(handle, 1);

            _cachedPreparedCommands = new ConcurrentCommandDictionary();
			_open = true;

			StoreDateTimeAsTicks = storeDateTimeAsTicks;

			BusyTimeout = TimeSpan.FromSeconds (0.1);

			Tracer = line => Debug.WriteLine (line);
		}

		/// <summary>
		/// Convert an input string to a quoted SQL string that can be safely used in queries.
		/// </summary>
		/// <returns>The quoted string.</returns>
		/// <param name="unsafeString">The unsafe string to quote.</param>
		private static string Quote (string unsafeString)
		{
			// TODO: Doesn't call sqlite3_mprintf("%Q", u) because we're waiting on https://github.com/ericsink/SQLitePCL.raw/issues/153
			if (unsafeString == null) return "NULL";
			var safe = unsafeString.Replace ("'", "''");
			return "'" + safe + "'";
		}

		/// <summary>
		/// Sets the key used to encrypt/decrypt the database.
		/// This must be the first thing you call before doing anything else with this connection
		/// if your database is encrypted.
		/// This only has an effect if you are using the SQLCipher nuget package.
		/// </summary>
		/// <param name="key">Ecryption key plain text that is converted to the real encryption key using PBKDF2 key derivation</param>
		public void SetKey (string key)
		{
			if (key == null) throw new ArgumentNullException (nameof (key));
			var q = Quote (key);
			Execute ("pragma key = " + q);
		}

		/// <summary>
		/// Sets the key used to encrypt/decrypt the database.
		/// This must be the first thing you call before doing anything else with this connection
		/// if your database is encrypted.
		/// This only has an effect if you are using the SQLCipher nuget package.
		/// </summary>
		/// <param name="key">256-bit (32 byte) ecryption key data</param>
		public void SetKey (byte[] key)
		{
			if (key == null) throw new ArgumentNullException (nameof (key));
			if (key.Length != 32) throw new ArgumentException ("Key must be 32 bytes (256-bit)", nameof(key));
			var s = String.Join ("", key.Select (x => x.ToString ("X2")));
			Execute ("pragma key = \"x'" + s + "'\"");
		}

		/// <summary>
		/// Enable or disable extension loading.
		/// </summary>
		public void EnableLoadExtension (bool enabled)
		{
			Result r = SQLite3.EnableLoadExtension (Handle, enabled ? 1 : 0);
			if (r != Result.OK) {
                string msg = SQLite3.GetErrorMessageUTF8(Handle);
				throw new SQLiteException(r, msg);
			}
		}

		/// <summary>
		/// Sets a busy handler to sleep the specified amount of time when a table is locked.
		/// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
		/// </summary>
		public TimeSpan BusyTimeout {
			get { return _busyTimeout; }
			set {
				_busyTimeout = value;
				if (Handle != NullHandle) {
					SQLite3.BusyTimeout (Handle, (int)_busyTimeout.TotalMilliseconds);
				}
			}
		}

		/// <summary>
		/// Returns the mappings from types to tables that the connection
		/// currently understands.
		/// </summary>
        public IEnumerable<TableMapping> TableMappings => TableMapping.TableMappings;

		/// <summary>
		/// Retrieves the mapping that is automatically generated for the given type.
		/// </summary>
		/// <param name="type">
		/// The type whose mapping to the database is returned.
		/// </param>
		/// <param name="createFlags">
		/// Optional flags allowing implicit PK and indexes based on naming conventions
		/// </param>
		/// <returns>
		/// The mapping represents the schema of the columns of the database and contains
		/// methods to set and get properties of objects.
		/// </returns>
        public TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None) => TableMapping.GetMapping(type, createFlags);

		/// <summary>
		/// Retrieves the mapping that is automatically generated for the given type.
		/// </summary>
		/// <returns>
		/// The mapping represents the schema of the columns of the database and contains
		/// methods to set and get properties of objects.
		/// </returns>
        public TableMapping GetMapping<T>() => TableMapping.GetMapping<T>();

		/// <summary>
		/// Executes a "drop table" on the database.  This is non-recoverable.
		/// </summary>
		public int DropTable<T> () => DropTable(GetMapping<T>());

		/// <summary>
		/// Executes a "drop table" on the database.  This is non-recoverable.
		/// </summary>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		public int DropTable (TableMapping map)
		{
			return Execute($"drop table if exists \"{map.TableName}\"");
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated.
		/// </returns>
		public CreateTableResult CreateTable<T> (CreateFlags createFlags = CreateFlags.None) => CreateTable(typeof (T), createFlags);

		/// <summary>
		/// Executes a "create table if not exists" on the database. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <param name="ty">Type to reflect to a database table.</param>
		/// <param name="createFlags">Optional flags allowing implicit PK and indexes based on naming conventions.</param>
		/// <returns>
		/// Whether the table was created or migrated.
		/// </returns>
		public CreateTableResult CreateTable (Type ty, CreateFlags createFlags = CreateFlags.None)
		{
			var map = GetMapping (ty, createFlags);

			// Present a nice error if no columns specified
			if (map.Columns.Length == 0) {
				throw new Exception (string.Format ("Cannot create a table without columns (does '{0}' have public properties?)", ty.FullName));
			}

			// Check if the table exists
			var result = CreateTableResult.Created;
			var existingCols = GetTableInfo (map.TableName);

            string SqlType (TableMapping.Column p, bool storeDateTimeAsTicks)
            {
                var clrType = p.ColumnType;
                if (clrType == typeof (Boolean) || clrType == typeof (Byte) || clrType == typeof (UInt16) || clrType == typeof (SByte) || clrType == typeof (Int16) || clrType == typeof (Int32) || clrType == typeof (UInt32) || clrType == typeof (Int64)) {
                    return "integer";
                }
                else if (clrType == typeof (Single) || clrType == typeof (Double) || clrType == typeof (Decimal)) {
                    return "float";
                }
                else if (clrType == typeof (String) || clrType == typeof (StringBuilder) || clrType == typeof (Uri) || clrType == typeof (UriBuilder)) {
                    int? len = p.MaxStringLength;

                    if (len.HasValue)
                        return "varchar(" + len.Value + ")";

                    return "varchar";
                }
                else if (clrType == typeof (TimeSpan)) {
                    return "bigint";
                }
                else if (clrType == typeof (DateTime)) {
                    return storeDateTimeAsTicks ? "bigint" : "datetime";
                }
                else if (clrType == typeof (DateTimeOffset)) {
                    return "bigint";
                }
                else if (clrType == typeof (byte[])) {
                    return "blob";
                }
                else if (clrType == typeof (Guid)) {
                    return "varchar(36)";
                }
                else {
                    var enumInfo = EnumCache.GetInfo(clrType);
                    if (enumInfo.IsEnum)
                    {
                        if (enumInfo.StoreAsText)
                            return "varchar";
                        else
                            return "integer";
                    }

                    throw new NotSupportedException ("Cannot store type: " + clrType);
                }
            };

            string SqlDecl (TableMapping.Column p, bool storeDateTimeAsTicks)
            {
                string decl = "\"" + p.Name + "\" " + SqlType (p, storeDateTimeAsTicks) + " ";

                if (p.IsPK) {
                    decl += "primary key ";
                }
                if (p.IsAutoInc) {
                    decl += "autoincrement ";
                }
                if (!p.IsNullable) {
                    decl += "not null ";
                }
                if (!string.IsNullOrEmpty (p.Collation)) {
                    decl += "collate " + p.Collation + " ";
                }
                if (p.HasDefaultValue)
                    decl += $"default(\'{p.DefaultValue}\')";

                return decl;
            };

			// Create or migrate it
			if (existingCols.Count == 0) {

				// Facilitate virtual tables a.k.a. full-text search.
				bool fts3 = (createFlags & CreateFlags.FullTextSearch3) != 0;
				bool fts4 = (createFlags & CreateFlags.FullTextSearch4) != 0;
				bool fts = fts3 || fts4;
				var @virtual = fts ? "virtual " : string.Empty;
				var @using = fts3 ? "using fts3 " : fts4 ? "using fts4 " : string.Empty;

				// Build query.
				var query = "create " + @virtual + "table if not exists \"" + map.TableName + "\" " + @using + "(\n";
				var decls = map.Columns.Select (p => SqlDecl (p, StoreDateTimeAsTicks));
				var decl = string.Join (",\n", decls.ToArray ());
				query += decl;
				query += ")";

				if(map.WithoutRowId) {
					query += " without rowid";
				}

                Result r;
                using (var cmd = NewCommand(query))
                    r = cmd.Execute(null);

                result = r == Result.Done ? CreateTableResult.Created : CreateTableResult.Error;
			}
			else {
				result = CreateTableResult.Migrated;

                var toBeAdded = new List<TableMapping.Column> ();

                foreach (var p in map.Columns) {
                    var found = false;
                    foreach (var c in existingCols) {
                        found = (string.Compare (p.Name, c.Name, StringComparison.OrdinalIgnoreCase) == 0);
                        if (found)
                            break;
                    }
                    if (!found) {
                        toBeAdded.Add (p);
                    }
                }

                Result r = Result.Done;
                foreach (var p in toBeAdded) {
                    var addCol = "alter table \"" + map.TableName + "\" add column " + SqlDecl (p, StoreDateTimeAsTicks);

                    using (var cmd = NewCommand(addCol))
                        r = cmd.Execute(null);

                    if (r != Result.Done)
                        break;
                }

                result = r == Result.Done ? CreateTableResult.Migrated : CreateTableResult.ErrorMigrating;
            }

            var success = true;
            var indexes = map.GetIndexs();
            foreach (var index in indexes)
            {
                var columns = index.Columns.OrderBy(i => i.Order).Select(i => i.ColumnName).ToArray();
                success = success && CreateIndex(index.IndexName, index.TableName, columns, index.Unique);
            }

            result = success ? result : CreateTableResult.Error;
            return result;
        }

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2> (CreateFlags createFlags = CreateFlags.None)
			where T : new()
			where T2 : new()
		{
			return CreateTables (createFlags, typeof (T), typeof (T2));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3> (CreateFlags createFlags = CreateFlags.None)
			where T : new()
			where T2 : new()
			where T3 : new()
		{
			return CreateTables (createFlags, typeof (T), typeof (T2), typeof (T3));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3, T4> (CreateFlags createFlags = CreateFlags.None)
			where T : new()
			where T2 : new()
			where T3 : new()
			where T4 : new()
		{
			return CreateTables (createFlags, typeof (T), typeof (T2), typeof (T3), typeof (T4));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3, T4, T5> (CreateFlags createFlags = CreateFlags.None)
			where T : new()
			where T2 : new()
			where T3 : new()
			where T4 : new()
			where T5 : new()
		{
			return CreateTables (createFlags, typeof (T), typeof (T2), typeof (T3), typeof (T4), typeof (T5));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables (CreateFlags createFlags = CreateFlags.None, params Type[] types)
		{
			var result = new CreateTablesResult ();
			foreach (Type type in types) {
				var aResult = CreateTable (type, createFlags);
				result.Results[type] = aResult;
			}
			return result;
		}

		/// <summary>
		/// Creates an index for the specified table and columns.
		/// </summary>
		/// <param name="indexName">Name of the index to create</param>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnNames">An array of column names to index</param>
		/// <param name="unique">Whether the index should be unique</param>
        public bool CreateIndex(string indexName, string tableName, string[] columnNames, bool unique = false)
        {
            const string sqlFormat = "create {2} index if not exists \"{3}\" on \"{0}\"(\"{1}\")";
            var sql = String.Format(sqlFormat, tableName, string.Join("\", \"", columnNames), unique ? "unique" : "", indexName);
            Result r;

            using (var cmd = NewCommand(sql))
                r = cmd.Execute(null);

            return r == Result.Done;
        }

		/// <summary>
		/// Creates an index for the specified table and column.
		/// </summary>
		/// <param name="indexName">Name of the index to create</param>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnName">Name of the column to index</param>
		/// <param name="unique">Whether the index should be unique</param>
        public bool CreateIndex(string indexName, string tableName, string columnName, bool unique = false) => CreateIndex (indexName, tableName, new string[] { columnName }, unique);

		/// <summary>
		/// Creates an index for the specified table and column.
		/// </summary>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnName">Name of the column to index</param>
		/// <param name="unique">Whether the index should be unique</param>
        public bool CreateIndex(string tableName, string columnName, bool unique = false) => CreateIndex (tableName + "_" + columnName, tableName, columnName, unique);

		/// <summary>
		/// Creates an index for the specified table and columns.
		/// </summary>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnNames">An array of column names to index</param>
		/// <param name="unique">Whether the index should be unique</param>
        public bool CreateIndex(string tableName, string[] columnNames, bool unique = false) => CreateIndex (tableName + "_" + string.Join ("_", columnNames), tableName, columnNames, unique);

		/// <summary>
		/// Creates an index for the specified object property.
		/// e.g. CreateIndex&lt;Client&gt;(c => c.Name);
		/// </summary>
		/// <typeparam name="T">Type to reflect to a database table.</typeparam>
		/// <param name="property">Property to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		public bool CreateIndex<T> (Expression<Func<T, object>> property, bool unique = false)
		{
			MemberExpression mx;
			if (property.Body.NodeType == ExpressionType.Convert) {
				mx = ((UnaryExpression)property.Body).Operand as MemberExpression;
			}
			else {
				mx = (property.Body as MemberExpression);
			}
			var propertyInfo = mx.Member as PropertyInfo;
			if (propertyInfo == null) {
				throw new ArgumentException ("The lambda expression 'property' should point to a valid Property");
			}

			var propName = propertyInfo.Name;

			var map = GetMapping<T> ();
			var colName = map.FindColumnWithPropertyName (propName).Name;

			return CreateIndex (map.TableName, colName, unique);
		}

		[Preserve (AllMembers = true)]
		public class ColumnInfo
		{
            //public int cid { get; set; }

			[Column ("name")]
			public string Name { get; set; }

            [Column ("type")]
            public string ColumnType { get; set; }

            [Column ("notnull")]
            public int NotNull { get; set; }

            //public string dflt_value { get; set; }

            [Column ("pk")]
            public int PK { get; set; }

            public override string ToString() => $"{Name} ({ColumnType})";
        }

		/// <summary>
		/// Query the built-in sqlite table_info table for a specific tables columns.
		/// </summary>
		/// <returns>The columns contains in the table.</returns>
		/// <param name="tableName">Table name.</param>
		public List<ColumnInfo> GetTableInfo (string tableName)
		{
			return Query<ColumnInfo> ($"pragma table_info(\"{tableName}\")");
		}

        /// <summary>
        /// Creates a new SQLiteCommand. Can be overridden to provide a sub-class.
        /// </summary>
        /// <seealso cref="SQLiteCommand.OnExecutionStarted"/>
        /// <seealso cref="SQLiteCommand.OnInstanceCreated"/>
        /// <seealso cref="SQLiteCommand.OnExecutionEnded"/>
        protected virtual SQLiteCommand NewCommand(string cmdText) => new SQLiteCommand(this, cmdText);

        /// <summary>
        /// Creates a new SQLiteCommand given the command text with arguments. Place a '?'
        /// in the command text for each of the arguments.
        /// </summary>
        /// <param name="cmdText">
        /// The fully escaped SQL.
        /// </param>
        /// <param name="fromCache"></param>
        /// <returns>
        /// A <see cref="SQLiteCommand"/>
        /// </returns>
        public SQLiteCommand CreateCommand(string cmdText, bool fromCache = false)
		{
			if (!_open)
                throw new SQLiteException(Result.Error, "Cannot create commands from unopened database", sql: cmdText);

            SQLiteCommand cmd = null;
            if (fromCache)
                cmd = GetCachedCommand(cmdText);
            if (cmd == null)
                cmd = NewCommand(cmdText);
            return cmd;
        }

        private SQLiteCommand GetCachedCommand(string commandName)
        {
            SQLiteCommand cachedCommand;

            if (!_cachedPreparedCommands.TryGetValue(commandName, out cachedCommand))
            {
                var prepCmd = NewCommand(commandName);

                cachedCommand = prepCmd;
                if (!_cachedPreparedCommands.TryAdd(commandName, prepCmd))
                {
                    prepCmd.Dispose();
                    _cachedPreparedCommands.TryGetValue(commandName, out cachedCommand);
				}
            }
            return cachedCommand;
		}

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// Use this method instead of Query when you don't expect rows back. Such cases include
		/// INSERTs, UPDATEs, and DELETEs.
		/// You can set the Trace or TimeExecution properties of the connection
		/// to profile execution.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The number of rows modified in the database as a result of this execution.
		/// </returns>
		public int Execute (string query, params object[] args)
		{
            using (var cmd = NewCommand(query))
            {
                var result = cmd.ExecuteNonQuery(args);
                return result;
			}
		}

        /// <summary>
        /// in the command text for each of the arguments and then executes that command.
        /// Use this method instead of Query when you don't expect rows back. Such cases include
        /// INSERTs, UPDATEs, and DELETEs.
        /// You can set the Trace or TimeExecution properties of the connection
		/// </summary>
        public int PreparedExecute(string query, params object[] args)
        {
            var cmd = CreateCommand(query, fromCache: true);
            var result = cmd.ExecuteNonQuery(args);
            return result;
		}

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// Use this method when return primitive values.
		/// You can set the Trace or TimeExecution properties of the connection
		/// to profile execution.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The number of rows modified in the database as a result of this execution.
		/// </returns>
		public T ExecuteScalar<T> (string query, params object[] args)
		{
            using (var cmd = NewCommand(query))
            {
                var result = cmd.ExecuteScalar<T>(args);
                return result;
			}
		}

        public List<T> Query<T>(string query, params object[] args) where T : new()
        {
            using (var cmd = CreateCommand(query, fromCache: false))
            {
                var result = cmd.ExecuteQuery<T>(args).ToList();
                return result;
			}
		}

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the mapping automatically generated for
		/// the given type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// </returns>
        public IEnumerable<T> PreparedQuery<T>(string query, params object[] args) where T : new()
		{
            var cmd = CreateCommand(query, fromCache: true);
            var result = cmd.ExecuteQuery<T>(args);
            return result;
		}

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the mapping automatically generated for
		/// the given type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
		/// will call sqlite3_step on each call to MoveNext, so the database
		/// connection must remain open for the lifetime of the enumerator.
		/// </returns>
		public IEnumerable<T> DeferredQuery<T> (string query, params object[] args) where T : new()
		{
            return PreparedQuery<T>(query, args);
		}

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the specified mapping. This function is
		/// only used by libraries in order to query the database via introspection. It is
		/// normally not used.
		/// </summary>
		/// <param name="map">
		/// A <see cref="TableMapping"/> to use to convert the resulting rows
		/// into objects.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// </returns>
		public List<object> Query (TableMapping map, string query, params object[] args)
		{
            using (var cmd = CreateCommand(query, fromCache: false))
            {
                var result = cmd.ExecuteQuery<object>(map, args).ToList();
                return result;
			}
        }

		/// <summary>
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the specified mapping. This function is
		/// only used by libraries in order to query the database via introspection. It is
		/// normally not used.
		/// </summary>
		/// <param name="map">
		/// A <see cref="TableMapping"/> to use to convert the resulting rows
		/// into objects.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// </returns>
        public IEnumerable<object> PreparedQuery(TableMapping map, string query, params object[] args)
        {
            var cmd = CreateCommand(query, fromCache: true);
            var result = cmd.ExecuteQuery<object>(map, args);
            return result;
        }

		/// <summary>
		/// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
		/// will call sqlite3_step on each call to MoveNext, so the database
		/// connection must remain open for the lifetime of the enumerator.
		/// </summary>
		public IEnumerable<object> DeferredQuery (TableMapping map, string query, params object[] args)
		{
            return PreparedQuery(map, query, args);
		}

		/// <summary>
		/// Returns a queryable interface to the table represented by the given type.
		/// </summary>
		/// <returns>
		/// A queryable object that is able to translate Where, OrderBy, and Take
		/// queries into native SQL.
		/// </returns>
		public TableQuery<T> Table<T> () where T : new()
		{
			return new TableQuery<T> (this);
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <returns>
		/// The object with the given primary key. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public T Get<T> (object pk) where T : new()
		{
			var map = GetMapping (typeof (T));
			return Query<T> (map.GetByPrimaryKeySql, pk).First ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The object with the given primary key. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public object Get (object pk, TableMapping map)
		{
			return Query (map, map.GetByPrimaryKeySql, pk).First ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the predicate from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="predicate">
		/// A predicate for which object to find.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public T Get<T> (Expression<Func<T, bool>> predicate) where T : new()
		{
			return Table<T> ().Where (predicate).First ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <returns>
		/// The object with the given primary key or null
		/// if the object is not found.
		/// </returns>
		public T Find<T> (object pk) where T : new()
		{
			var map = GetMapping (typeof (T));
			return Query<T> (map.GetByPrimaryKeySql, pk).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The object with the given primary key or null
		/// if the object is not found.
		/// </returns>
		public object Find (object pk, TableMapping map)
		{
			return Query (map, map.GetByPrimaryKeySql, pk).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the predicate from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="predicate">
		/// A predicate for which object to find.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public T Find<T> (Expression<Func<T, bool>> predicate) where T : new()
		{
			return Table<T> ().Where (predicate).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the query from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public T FindWithQuery<T> (string query, params object[] args) where T : new()
		{
			return Query<T> (query, args).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the query from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public object FindWithQuery (TableMapping map, string query, params object[] args)
		{
			return Query (map, query, args).FirstOrDefault ();
		}

		/// <summary>
		/// Whether <see cref="BeginTransaction"/> has been called and the database is waiting for a <see cref="Commit"/>.
		/// </summary>
		public bool IsInTransaction => _transactionDepth > 0;

		/// <summary>
		/// Begins a new transaction. Call <see cref="Commit"/> to end the transaction.
		/// </summary>
		/// <example cref="System.InvalidOperationException">Throws if a transaction has already begun.</example>
		public void BeginTransaction ()
		{
			// The BEGIN command only works if the transaction stack is empty,
			//    or in other words if there are no pending transactions.
			// If the transaction stack is not empty when the BEGIN command is invoked,
			//    then the command fails with an error.
			// Rather than crash with an error, we will just ignore calls to BeginTransaction
			//    that would result in an error.
			if (Interlocked.CompareExchange (ref _transactionDepth, 1, 0) == 0) {
				try {
					Execute ("begin transaction");
				}
				catch (Exception ex) {
					var sqlExp = ex as SQLiteException;
					if (sqlExp != null) {
						// It is recommended that applications respond to the errors listed below
						//    by explicitly issuing a ROLLBACK command.
						// TODO: This rollback failsafe should be localized to all throw sites.
						switch (sqlExp.Result) {
							case Result.IOError:
							case Result.Full:
							case Result.Busy:
							case Result.NoMem:
							case Result.Interrupt:
								RollbackTo (null, true);
								break;
						}
					}
					else {
						// Call decrement and not VolatileWrite in case we've already
						//    created a transaction point in SaveTransactionPoint since the catch.
						Interlocked.Decrement (ref _transactionDepth);
					}

					throw;
				}
			}
			else {
				// Calling BeginTransaction on an already open transaction is invalid
				throw new InvalidOperationException ("Cannot begin a transaction while already in a transaction.");
			}
		}

		/// <summary>
		/// Creates a savepoint in the database at the current point in the transaction timeline.
		/// Begins a new transaction if one is not in progress.
		///
		/// Call <see cref="RollbackTo"/> to undo transactions since the returned savepoint.
		/// Call <see cref="Release"/> to commit transactions after the savepoint returned here.
		/// Call <see cref="Commit"/> to end the transaction, committing all changes.
		/// </summary>
		/// <returns>A string naming the savepoint.</returns>
		public string SaveTransactionPoint ()
		{
			int depth = Interlocked.Increment (ref _transactionDepth) - 1;
			string retVal = "S" + _rand.Next (short.MaxValue) + "D" + depth;

			try {
				Execute ("savepoint " + retVal);
			}
			catch (Exception ex) {
				var sqlExp = ex as SQLiteException;
				if (sqlExp != null) {
					// It is recommended that applications respond to the errors listed below
					//    by explicitly issuing a ROLLBACK command.
					// TODO: This rollback failsafe should be localized to all throw sites.
					switch (sqlExp.Result) {
						case Result.IOError:
						case Result.Full:
						case Result.Busy:
						case Result.NoMem:
						case Result.Interrupt:
							RollbackTo (null, true);
							break;
					}
				}
				else {
					Interlocked.Decrement (ref _transactionDepth);
				}

				throw;
			}

			return retVal;
		}

		/// <summary>
		/// Rolls back the transaction that was begun by <see cref="BeginTransaction"/> or <see cref="SaveTransactionPoint"/>.
		/// </summary>
        public void Rollback(bool noThrow = false) => RollbackTo(null, noThrow);

        /// <summary>
        /// Rolls back the savepoint created by <see cref="BeginTransaction"/> or SaveTransactionPoint.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/></param>
        /// <param name="noThrow"></param>
        public void RollbackTo(string savepoint, bool noThrow = false)
        {
            // Rolling back without a TO clause rolls backs all transactions
            //    and leaves the transaction stack empty.
            try
            {
                if (String.IsNullOrEmpty(savepoint))
                {
                    if (Interlocked.Exchange(ref _transactionDepth, 0) > 0)
                    {
                        Execute("rollback");
                    }
                }
                else
                {
                    DoSavePointExecute(savepoint, "rollback to ");
                }
            }
            catch (SQLiteException)
            {
                if (!noThrow)
                    throw;
            }
            // No need to rollback if there are no transactions open.
        }

        /// <summary>
        /// Releases a savepoint returned from <see cref="SaveTransactionPoint"/>.  Releasing a savepoint
        ///    makes changes since that savepoint permanent if the savepoint began the transaction,
        ///    or otherwise the changes are permanent pending a call to <see cref="Commit"/>.
        ///
        /// The RELEASE command is like a COMMIT for a SAVEPOINT.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to release.  The string should be the result of a call to <see cref="SaveTransactionPoint"/></param>
        /// <param name="rollbackOnFailure"></param>
        public void Release (string savepoint, bool rollbackOnFailure = true)
		{
			try
			{
				DoSavePointExecute(savepoint, "release ");
			}
			catch (SQLiteException ex)
			{
				if (rollbackOnFailure && ex.Result == Result.Busy)
				{
					// Force a rollback since most people don't know this function can fail
					// Don't call Rollback() since the _transactionDepth is 0 and it won't try
					// Calling rollback makes our _transactionDepth variable correct.
					// Writes to the database only happen at depth=0, so this failure will only happen then.
					try
					{
						Execute("rollback");
					}
					catch
					{
						// rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
					}
				}

				throw;
			}
		}

        private void DoSavePointExecute(string savepoint, string cmd)
		{
			// Validate the savepoint
			int firstLen = savepoint.IndexOf ('D');
			if (firstLen >= 2 && savepoint.Length > firstLen + 1) {
				int depth;
				if (Int32.TryParse (savepoint.Substring (firstLen + 1), out depth)) {
					// TODO: Mild race here, but inescapable without locking almost everywhere.
					if (0 <= depth && depth < _transactionDepth) {
						Volatile.Write (ref _transactionDepth, depth);
						Execute (cmd + savepoint);
						return;
					}
				}
			}

			throw new ArgumentException ("savePoint is not valid, and should be the result of a call to SaveTransactionPoint.", "savePoint");
		}

		/// <summary>
		/// Commits the transaction that was begun by <see cref="BeginTransaction"/>.
		/// </summary>
		public void Commit (bool rollbackOnFailure = true)
		{
			if (Interlocked.Exchange (ref _transactionDepth, 0) != 0) {
				try
				{
					Execute("commit");
				}
				catch
				{
					if (rollbackOnFailure)
					{
						// Force a rollback since most people don't know this function can fail
						// Don't call Rollback() since the _transactionDepth is 0 and it won't try
						// Calling rollback makes our _transactionDepth variable correct.
						try
						{
							Execute("rollback");
						}
						catch
						{
							// rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
						}
					}

					throw;
				}
			}
			// Do nothing on a commit with no open transaction
		}

		/// <summary>
		/// Executes <paramref name="action"/> within a (possibly nested) transaction by wrapping it in a SAVEPOINT. If an
		/// exception occurs the whole transaction is rolled back, not just the current savepoint. The exception
		/// is rethrown.
		/// </summary>
		/// <param name="action">
		/// The <see cref="Action"/> to perform within a transaction. <paramref name="action"/> can contain any number
		/// of operations on the connection but should never call <see cref="BeginTransaction"/> or
		/// <see cref="Commit"/>.
		/// </param>
		public void RunInTransaction (Action action)
		{
			try {
				var savePoint = SaveTransactionPoint ();
				action ();
				Release (savePoint);
			}
			catch (Exception) {
				Rollback ();
				throw;
			}
		}

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, bool runInTransaction = true)
		{
            return InsertAll(objects, "", null, runInTransaction);
		}

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, string extra, bool runInTransaction = true)
		{
            return InsertAll(objects, extra, null, runInTransaction);
        }

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, Type objType, bool runInTransaction = true)
		{
            return InsertAll(objects, "", objType, runInTransaction);
		}

	    private static readonly SQLiteVersion MinInsertAllVersion = new SQLiteVersion(3007011); // 3.7.11

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
        /// <param name="objType">
        /// The type of object to insert.
        /// </param>
        /// <param name="runInTransaction">
        /// A boolean indicating if the inserts should be wrapped in a transaction.
        /// </param>
        /// <returns>
        /// The number of rows added to the table.
        /// </returns>
        public int InsertAll(System.Collections.IEnumerable objects, string extra, Type objType, bool runInTransaction = true)
        {
            var useFallback = false;
            var count = 0;

            if (LibraryVersion < MinInsertAllVersion)
                useFallback = true;

            if (!useFallback)
            {
		        var o = new List<object> ();
	            foreach (var r in objects)
	                if (r != null)
	                    o.Add(r);

	            // if there are no records just return 0
	            if (o.Count == 0)
	                return 0;

	            // if there is only one object it is better to go through the old logic because the insert is already prepared
                if (!useFallback && o.Count == 1)
                    useFallback = true;

                var firstRecordType = objType ?? o[0].GetType();

                if (!useFallback)
	            {
	                // all the types need to match
	                foreach (var r in o)
	                {
	                    if (r.GetType() != firstRecordType)
	                    {
                            useFallback = true;
	                        break;
	                    }
	                }
	            }

                if (!useFallback)
                {
                    var map = GetMapping(firstRecordType);
                    var replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;
                    var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;

                    useFallback = cols.Length == 0;
                }

                if (!useFallback)
	            {
	                if (runInTransaction)
	                {
                        RunInTransaction(() => { count = InternalInsertAll(o, extra, objType); });
	                }
	                else
	                {
	                    count = InternalInsertAll(o, extra, objType);
	                }
	            }
	        }

            if (useFallback)
	        {
	            if (runInTransaction)
	            {
	                RunInTransaction(() => {
	                    foreach (var r in objects)
	                        count += Insert(r, extra, objType);
	                });
	            }
	            else
	            {
	                foreach (var r in objects)
	                    count += Insert(r, extra, objType);
	            }
	        }

	        return count;
	    }

	    private object[] InternalInsertRecordValues(TableMapping map, object obj, Type objType, bool replacing)
	    {
			if (map.PK != null && map.PK.IsAutoGuid)
			{
				// no GetProperty so search our way up the inheritance chain till we find it
				PropertyInfo prop;
				while (objType != null)
				{
					var info = objType.GetTypeInfo();
					prop = info.GetDeclaredProperty(map.PK.PropertyName);
					if (prop != null)
					{
						if (prop.GetValue(obj, null).Equals(Guid.Empty))
						{
							prop.SetValue(obj, Guid.NewGuid(), null);
						}
						break;
					}

					objType = info.BaseType;
				}
			}

	        var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;
            var vals = new object[cols.Length];

            for (var i = 0; i < vals.Length; i++)
            {
                var col = cols[i];
                var value = col.GetValue(obj);

                // primary key, auto increment, with a value of 0 should be considered a new incremented value
                if (col.IsPK && col.IsAutoInc && col.ColumnType == typeof(long) && Object.Equals(value, 0L))
                    value = null;

                // if value is null and column has default attribute
                if (value == null && col.HasDefaultValue)
                    value = col.DefaultValue;

                vals[i] = value;
            }

            return vals;
	    }

        private int InternalInsertAll(IList objects, string extra, Type objType)
        {
            if (objects.Count == 0)
                return 0;

            objType = objType ?? objects[0].GetType();

	        int count = 0;
            var map = GetMapping(objType);
	        var replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;
	        var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;

            // todo: look up if this value was changed at runtime by using sqlite3_limit(db,SQLITE_LIMIT_VARIABLE_NUMBER,size)
            const int SQLITE_LIMIT_VARIABLE_NUMBER = 999;

	        var recordValues = new Queue<object[]>();
            foreach(var obj in objects)
                recordValues.Enqueue(InternalInsertRecordValues(map, obj, objType, replacing));

            var line = $"({String.Join(",", cols.Select(c => "?"))}),";
            while (recordValues.Count > 0)
	        {
	            var insertVals = new List<object>();
                var colNames = $"({String.Join(",", cols.Select(c => $"\"{c.Name}\""))})";
                var s = new StringBuilder();
                s.AppendFormat("insert {2} into \"{0}\"{1} values ", map.TableName, colNames, extra);

                while (recordValues.Count > 0 && insertVals.Count + cols.Length < SQLITE_LIMIT_VARIABLE_NUMBER)
                {
                    insertVals.AddRange(recordValues.Dequeue());
                    s.Append(line);
                }

                // remove the last comma from the string
                var insertSql = s.ToString(0, s.Length - 1);

                try
                {
                    count += Execute(insertSql, insertVals.ToArray());
                }
				catch (SQLiteException ex)
				{
					ex.PopulateColumnFromTableMapping(map);

					throw;
				}
			}

            if (count > 0)
                OnTableChanged(map, NotifyTableChangedAction.Insert, count);

            return count;
        }

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj)
		{
			return Insert (obj, null, null);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// If a UNIQUE constraint violation occurs with
		/// some pre-existing object, this function deletes
		/// the old object.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int InsertOrReplace (object obj)
		{
			return Insert (obj, "OR REPLACE", null);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, Type objType)
		{
			return Insert (obj, null, objType);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// If a UNIQUE constraint violation occurs with
		/// some pre-existing object, this function deletes
		/// the old object.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int InsertOrReplace (object obj, Type objType)
		{
			return Insert (obj, "OR REPLACE", objType);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, string extra)
		{
			return Insert (obj, extra, null);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, string extra, Type objType)
		{
			if (obj == null) {
				return 0;
			}

            extra = extra ?? "";
            objType = objType ?? Nullable.GetUnderlyingType(obj.GetType()) ?? obj.GetType();

            var map = GetMapping(objType);
            var replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;
            var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;
            var vals = InternalInsertRecordValues(map, obj, objType, replacing);

            var insertCmd = GetInsertCommand(map, extra);
            int count;

            lock (insertCmd)
            {
                // We lock here to protect the prepared statement returned via GetInsertCommand.
                // A SQLite prepared statement can be bound for only one operation at a time.
                try
                {
                    count = insertCmd.ExecuteNonQuery(vals);
                }
				catch (SQLiteException ex)
				{
					ex.PopulateColumnFromTableMapping(map);

					throw;
				}

				if (map.HasAutoIncPK) {
					var id = SQLite3.LastInsertRowid (Handle);
					map.SetAutoIncPK (obj, id);
				}
			}
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Insert);

			return count;
		}

        public SQLiteCommand GetInsertCommand(TableMapping map, string extra)
        {
            var commandName = $"insert {extra} into \"{map.TableName}\"";

            SQLiteCommand cachedCommand;

            if (!_cachedPreparedCommands.TryGetValue(commandName, out cachedCommand))
            {
                var cols = map.InsertColumns;
                string insertSql;

                // weird case to handle where there is only one column in the database, and it is an auto incrementing column
                if (!cols.Any() && map.Columns.Count() == 1 && map.Columns[0].IsAutoInc)
                {
                    insertSql = $"insert {extra} into \"{map.TableName}\" default values";
                }
                else
                {
                    if (String.Equals(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase))
                        cols = map.InsertOrReplaceColumns;

                    insertSql = String.Format("insert {3} into \"{0}\"({1}) values ({2})", map.TableName,
                        String.Join(",", cols.Select(c => $"\"{c.Name}\"")),
                        String.Join(",", cols.Select(c => "?")), extra);
                }

                var prepCmd = new SQLiteCommand(this, insertSql);

                cachedCommand = prepCmd;
                if (!_cachedPreparedCommands.TryAdd(commandName, prepCmd))
                {
                    // concurrent add attempt this add, retreive a fresh copy
                    prepCmd.Dispose();
                    _cachedPreparedCommands.TryGetValue(commandName, out cachedCommand);
                }
            }

            return cachedCommand;
        }

		/// <summary>
		/// Updates all of the columns of a table using the specified object
		/// except for its primary key.
		/// The object is required to have a primary key.
		/// </summary>
		/// <param name="obj">
		/// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
		/// </param>
		/// <returns>
		/// The number of rows updated.
		/// </returns>
		public int Update (object obj)
		{
            return Update(obj, null, null, null);
		}

		/// <summary>
		/// Updates all of the columns of a table using the specified object
		/// except for its primary key.
		/// The object is required to have a primary key.
		/// </summary>
		/// <param name="obj">
		/// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
		/// </param>
        /// <param name="updateKey">
        /// The column name to use as the unique index to update the record.  (defaults to the primary key)
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj, string updateKey)
        {
            return Update(obj, null, null, updateKey);
        }

        /// <summary>
        /// Updates all of the columns of a table using the specified object
        /// except for its primary key.
        /// The object is required to have a primary key.
        /// </summary>
        /// <param name="obj">
        /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <param name="objType">
        /// The type of object to update.
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj, Type objType)
        {
            return Update(obj, null, objType, null);
        }

        /// <summary>
        /// Updates all of the columns of a table using the specified object
        /// except for its primary key.
        /// The object is required to have a primary key.
        /// </summary>
        /// <param name="obj">
        /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <param name="objType">
        /// The type of object to update.
        /// </param>
        /// <param name="updateKey">
        /// The column name to use as the unique index to update the record.  (defaults to the primary key)
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj, Type objType, string updateKey)
        {
            return Update(obj, null, objType, updateKey);
        }

        /// <summary>
        /// Updates all of the columns of a table using the specified object
        /// except for its primary key.
        /// The object is required to have a primary key.
        /// </summary>
        /// <param name="obj">
        /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <param name="extra"></param>
        /// <param name="objType">
        /// The type of object to update.
        /// </param>
        /// <param name="updateKey">
        /// The column name to use as the unique index to update the record.  (defaults to the primary key)
        /// </param>
        /// <returns>
        /// The number of rows updated.
        /// </returns>
        public int Update(object obj, string extra, Type objType, string updateKey = null)
        {
			if (obj == null) {
				return 0;
			}

            extra = extra ?? "";
            objType = objType ?? Nullable.GetUnderlyingType(obj.GetType()) ?? obj.GetType();

            var map = GetMapping(objType);
            var cols = map.UpdateColumns;

            var keyCol = updateKey == null ? map.PK : cols.Where(x => x.Name == updateKey).FirstOrDefault() ?? map.PK;

            if (keyCol == null || !keyCol.IsUnique)
                throw new NotSupportedException($"Cannot use {updateKey ?? "the primary key"} to update {map.TableName} it is not unique.");

            var vals = cols.Where(x => x != keyCol).Select(x => x.GetValue(obj));

            var ps = new List<object>(vals);

            if (ps.Count == 0)
            {
                // There is a PK but no accompanying data,
                // so reset the PK to make the UPDATE work.
                cols = map.Columns;
                vals = from c in cols
                       select c.GetValue(obj);
                ps = new List<object>(vals);
            }

            // add the primary key as the last value
            ps.Add(keyCol.GetValue(obj));

            var updateCmd = GetUpdateCommand(map, extra);
            int count;

            lock (updateCmd)
            {
                // We lock here to protect the prepared statement returned via GetInsertCommand.
                // A SQLite prepared statement can be bound for only one operation at a time.
                try
                {
                    count = updateCmd.ExecuteNonQuery(ps.ToArray());
                }
                catch (SQLiteException ex)
                {
					ex.PopulateColumnFromTableMapping(map);

					throw;
				}
            }

            if (count > 0)
                OnTableChanged(map, NotifyTableChangedAction.Update, count);

            return count;
        }

        public SQLiteCommand GetUpdateCommand(TableMapping map, string extra, string updateKey = null)
        {
            var commandName = $"update {extra} \"{map.TableName}\" with \"{updateKey ?? "PK"}\"";

            SQLiteCommand cachedCommand;

            if (!_cachedPreparedCommands.TryGetValue(commandName, out cachedCommand))
            {
                var keyCol = updateKey == null ? map.PK : map.UpdateColumns.Where(x => x.Name == updateKey).FirstOrDefault() ?? map.PK;

                if (keyCol == null || !keyCol.IsUnique)
                    throw new NotSupportedException($"Cannot use {updateKey ?? "the primary key"} to update {map.TableName} it is not unique.");

                var cols = map.UpdateColumns.Where(x => x != keyCol).ToList();

                // handle the primary key only used case
                // that I don't know who in their right mind would do
                if (cols.Count == 0)
                    cols = map.Columns.ToList();

                string updateSql;

                updateSql = String.Format("update {3} \"{0}\" set {1} where {2} = ?", map.TableName,
                    String.Join(",", cols.Select(c => $"\"{c.Name}\" = ?")),
                    keyCol.Name, extra);

                var prepCmd = new SQLiteCommand(this, updateSql);

                cachedCommand = prepCmd;
                if (!_cachedPreparedCommands.TryAdd(commandName, prepCmd))
                {
                    // concurrent add attempt this add, retreive a fresh copy
                    prepCmd.Dispose();
                    _cachedPreparedCommands.TryGetValue(commandName, out cachedCommand);
                }
            }

            return cachedCommand;
        }

		/// <summary>
		/// Updates all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int UpdateAll (System.Collections.IEnumerable objects, bool runInTransaction = true)
		{
			var c = 0;
			if (runInTransaction) {
				RunInTransaction (() => {
					foreach (var r in objects) {
						c += Update (r);
					}
				});
			}
			else {
				foreach (var r in objects) {
					c += Update (r);
				}
			}
			return c;
		}

		/// <summary>
		/// Deletes the given object from the database using its primary key.
		/// </summary>
        /// <param name="obj">
        /// The object to delete. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
		/// <returns>
		/// The number of rows deleted.
		/// </returns>
		public int Delete (object obj)
		{
            return Delete(obj, null);
        }

		/// <summary>
		/// Deletes the object with the specified primary key.
		/// </summary>
		/// <param name="primaryKey">
		/// The primary key of the object to delete.
		/// </param>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		/// <typeparam name='T'>
		/// The type of object.
		/// </typeparam>
		public int Delete<T> (object primaryKey)
		{
			return Delete (primaryKey, typeof(T));
		}

		/// <summary>
		/// Deletes the given object from the database using its primary key.
		/// </summary>
        /// <param name="obj">
        /// The object to delete. It must have a primary key designated using the PrimaryKeyAttribute.
        /// </param>
        /// <param name="objType">
        /// The type of object to delete.
        /// </param>
		/// <returns>
		/// The number of rows deleted.
		/// </returns>
		public int Delete (object obj, Type objType)
		{
            if (obj == null) {
                return 0;
            }

            objType = objType ?? Nullable.GetUnderlyingType(obj.GetType()) ?? obj.GetType();

			var map = GetMapping (objType);
			var pk = map.PK;
			if (pk == null) {
				throw new NotSupportedException ("Cannot delete " + map.TableName + ": it has no PK");
			}
			var q = string.Format ("delete from \"{0}\" where \"{1}\" = ?", map.TableName, pk.Name);
            var arg = obj;

            // if the object is related to this table, then get the value from the property, else we assume the obj is the primary key value
            if (map.IsObjectRelated(obj))
                arg = pk.GetValue(obj);

            var count = PreparedExecute(q, arg);
            if (count > 0)
                OnTableChanged(map, NotifyTableChangedAction.Delete);

            return count;
        }

		/// <summary>
		/// Deletes all the objects from the specified table.
		/// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
		/// specified table. Do you really want to do that?
		/// </summary>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		/// <typeparam name='T'>
		/// The type of objects to delete.
		/// </typeparam>
		public int DeleteAll<T> ()
		{
			return DeleteAll (typeof(T));
		}

		/// <summary>
		/// Deletes all the objects from the specified table.
		/// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
		/// specified table. Do you really want to do that?
		/// </summary>
        /// <param name="objType">
        /// The type of object to delete.
        /// </param>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		public int DeleteAll (Type objType)
		{
			var map = GetMapping (objType);
			var query = string.Format ("delete from \"{0}\"", map.TableName);
            var count = PreparedExecute(query);
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Delete);

			return count;
		}

		~SQLiteConnection ()
		{
			Dispose (false);
		}

		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		public void Close ()
		{
			Dispose (true);
		}

		protected virtual void Dispose (bool disposing)
		{
			if (_open && Handle != NullHandle) {
				try {
					if (disposing) {
                        foreach (var pair in _cachedPreparedCommands)
                            pair.Value.Dispose();

                        _cachedPreparedCommands.Clear();
					}

					var r = SQLite3.Close(Handle);

					// only throw an exception if we are disposing, because it is bad practice to throw an exception in the finalizer
					if (disposing && r != Result.OK) {
						var msg = SQLite3.GetErrorMessageUTF8(Handle);
						throw new SQLiteException(r, msg);
					}
				}
				finally {
					Handle.Dispose();
					Handle = NullHandle;
					_open = false;
				}
			}
		}

        private void OnTableChanged(TableMapping table, NotifyTableChangedAction action, int count = 1)
		{
			var ev = TableChanged;
			if (ev != null)
				ev (this, new NotifyTableChangedEventArgs (table, action, count));
		}

		public event EventHandler<NotifyTableChangedEventArgs> TableChanged;
	}
}
