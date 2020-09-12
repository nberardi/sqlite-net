using System;
using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;

using SQLite;
using SQLitePCL;

namespace SQLite
{
    public enum SqlLogType
    {
        Debug,
        Info,
        Warning,
        Fatal
    }

    public abstract partial class DataAccessLayerBase : IDisposable
    {
        ///<summary>In memory database connection string</summary>
        public const string InMemoryDatabase = ":memory:";

        ///<summary>Get a named in memory database connection string</summary>
        public static string GetNamedInMemoryDatabase(string name) => String.Format("file:{0}?mode=memory", name);

        [Obsolete("Do not use the write lock direction, use GetWriteConnectionLock() instead.", error: false)]
        private readonly object _writeConnectionLock = new object();
        private volatile string _writeConnectionLockReason = null;

        private SQLiteConnection _writeConnection;
        private SQLiteConnectionPool _readPool;
        private IDisposable _startupChangedSubscription;

#if TEST
        public SQLiteConnection WriteConnection => _writeConnection;
        public SQLiteConnectionPool ReadPool => _readPool;
#endif

        ///<summary>The current library version.</summary>
        public SQLiteVersion LibraryVersion => SQLite3.LibraryVersion;

        ///<summary>The min version of SQLite that is supported.</summary>
        public virtual SQLiteVersion MinLibraryVersion => new SQLiteVersion(3000000); // 3.0.0 (should capture all SQLite3 versions)

        ///<summary>The database version, executes PRAGMA user_version</summary>
        public int DatabaseVersion
        {
            get { return Read(conn => conn.ExecuteScalar<int>("PRAGMA user_version;")); }
#if TEST
            set { Write(conn => conn.Execute($"PRAGMA user_version={value};")); }
#endif
        }

        ///<summary>Is this connection for an in memory database.</summary>
        public bool IsInMemoryDatabase { get; private set; } = false;

        ///<summary>The database page count, executes PRAGMA page_count</summary>
        public long DatabasePageCount => Read(conn => conn.ExecuteScalar<long>("PRAGMA page_count;"));

        ///<summary>The database page size, executes PRAGMA page_size</summary>
        public long DatabasePageSize => Read(conn => conn.ExecuteScalar<long>("PRAGMA page_size;"));

        ///<summary>The database memory used in bytes.</summary>
        public long DatabaseMemoryUsed
        {
            get
            {
                try
                {
                    return DatabasePageSize*DatabasePageCount;
                }
                catch
                {
                    return -1L;
                }
            }
        }

        ///<summary>This must be set to the latest database version to run migration when it changes.</summary>
        protected abstract int LatestDatabaseVersion { get; }

        ///<summary>The database path.</summary>
        public string DatabasePath { get; private set; }

        ///<summary>The database flags used when the connection was open.</summary>
        public SQLiteOpenFlags DatabaseOpenFlags { get; private set; }

        ///<summary>Get a write connection lock on the database.</summary>
        ///<param name="reason">The reason the connection lock was obtained.</param>
        public IDisposable GetWriteConnectionLock(string reason) => DatabaseWriteLock.Lock(this, reason, DatabaseWriteLockTimeout);

        ///<summary>Get a write connection lock on the database.</summary>
        ///<param name="reason">The reason the connection lock was obtained.</param>
        ///<param name="timeout">The max time the lock will be obtained before being freed.</param>
        public IDisposable GetWriteConnectionLock(string reason, TimeSpan timeout) => DatabaseWriteLock.Lock(this, reason, timeout);

		private Action<string> _tracer;
        private static Action<SqlLogType, string, Exception> _log;

		/// <summary>
		/// Whether to writer queries to <see cref="Tracer"/> during execution.
		/// </summary>
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
			set { _tracer = value ?? (line => Log(SqlLogType.Debug, line, null)); }
		}

		/// <summary>
		/// Sets a busy handler to sleep the specified amount of time when a table is locked.
		/// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
		/// </summary>
        public TimeSpan BusyTimeout { get; set; } = TimeSpan.FromSeconds(1);

        ///<summary>The default write connection lock timeout.</summary>
        public TimeSpan DatabaseWriteLockTimeout { get; set; } = TimeSpan.FromMinutes(1);

        ///<summary>The log action method.</summary>
        public static Action<SqlLogType, string, Exception> Log
 		{
			get => _log;
			set { _log = value ?? ((type, message, err) => Debug.WriteLine("[{0}] {1}\n{2}", type, message, err)); }
		}

        public string SQLite3LibraryName { get; set; } = "sqlite3";

        protected string EscapeString(string s)
        {
            return s?.Replace("'", "''");
        }

        protected DataAccessLayerBase()
        {

        }

        protected DataAccessLayerBase(string databasePath, SQLiteOpenFlags flags = SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create)
        {
            Init(databasePath, flags);
        }

#region Bulk Loading

        public bool BulkLoadStart()
        {
            using(GetWriteConnectionLock(nameof(BulkLoadStart)))
            {
                var currentConn = _writeConnection;

                // make sure page_size is the same in source and destination databases
                var sourcePageSize = Execute(() => currentConn.ExecuteScalar<long>("PRAGMA page_size;"));

                var conn = CreateConnection(InMemoryDatabase, DatabaseOpenFlags, sourcePageSize);
                var memoryConnection = conn.Item1;

                if (SwapConnections(memoryConnection, currentConn))
                {
                    SetConnection(conn);
                    return true;
                }

                conn.Item1?.Dispose();
                conn.Item2?.Dispose();

                return false;
            }
        }

        public bool BulkLoadRollback()
        {
            using (GetWriteConnectionLock(nameof(BulkLoadRollback)))
            {
                var conn = CreateConnection(DatabasePath, DatabaseOpenFlags);
                SetConnection(conn);
                return true;
            }
        }

        public bool BulkLoadFinish()
        {
            using (GetWriteConnectionLock(nameof(BulkLoadFinish)))
            {
                var currentConn = _writeConnection;

                Analyze(currentConn);

                // make sure page_size is the same in source and destination databases
                var sourcePageSize = Execute(() => currentConn.ExecuteScalar<long>("PRAGMA page_size;"));

                var conn = CreateConnection(DatabasePath, DatabaseOpenFlags, sourcePageSize);
                var diskConnection = conn.Item1;

                if (SwapConnections(diskConnection, currentConn))
                {
                    SetConnection(conn);
                    return true;
                }

                conn.Item1?.Dispose();
                conn.Item2?.Dispose();

                return false;
            }
        }

        private bool SwapConnections(SQLiteConnection destination, SQLiteConnection source)
        {
            using (GetWriteConnectionLock(nameof(SwapConnections)))
            {
                var backup = raw.sqlite3_backup_init(destination.Handle, "main", source.Handle, "main");
                if (backup != null)
                {
                    var step = raw.sqlite3_backup_step(backup, -1);
                    if (step != (int)Result.Done)
                        return false;

                    var finish = raw.sqlite3_backup_finish(backup);
                    if (finish != (int)Result.OK)
                        return false;

                    return true;
                }

                return false;
            }
        }

#endregion

#region Init

        protected void Init(string databasePath, SQLiteOpenFlags flags = SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create)
        {
            Log(SqlLogType.Info, String.Format("using SQLite version {version} for {type}", LibraryVersion, GetType().Name), null);

            #if !TEST
            if (LibraryVersion < MinLibraryVersion)
                throw new NotSupportedException($"SQLite version {LibraryVersion} is not supported. The only supported version is {MinLibraryVersion}.");
            #endif

            DatabasePath = databasePath;
            DatabaseOpenFlags = flags;

            var conn = CreateConnection(DatabasePath, DatabaseOpenFlags);
            SetConnection(conn);
        }

        private void SetConnection(Tuple<SQLiteConnection, SQLiteConnectionPool> conn)
        {
            using (GetWriteConnectionLock(nameof(SetConnection)))
            {
                _writeConnection?.Dispose();
                _writeConnection = conn.Item1;

                if (_writeConnection != null && _writeConnection.Handle != SQLiteConnection.NullHandle)
                {
                    var path = raw.sqlite3_db_filename(_writeConnection.Handle, "main").utf8_to_string();
                    IsInMemoryDatabase = String.IsNullOrWhiteSpace(path);
                }
            }

            var oldReadPool = _readPool;
            Interlocked.Exchange(ref _readPool, conn.Item2);
            oldReadPool?.Dispose();
        }

        private Tuple<SQLiteConnection, SQLiteConnectionPool> CreateConnection(string databasePath, SQLiteOpenFlags flags, long? pageSize = null)
        {
            var connectionPoolFlags = SQLiteOpenFlags.ReadOnly;

            // changed for a shared in memory database path
            if (databasePath == InMemoryDatabase)
            {
                databasePath = GetNamedInMemoryDatabase(GetType().Name);
                flags = flags | SQLiteOpenFlags.OpenUri;
                connectionPoolFlags = connectionPoolFlags | SQLiteOpenFlags.OpenUri;
            }

            var conn = new SQLiteConnection(databasePath, flags) {
                Trace = Trace,
                TraceTime = TraceTime,
                TraceTimeExceeding = TraceTimeExceeding,
                BusyTimeout = BusyTimeout,
                Tracer = l => Tracer($"Write Connection: {l}")
            };

            try
            {
                var result = Execute(() => conn.ExecuteScalar<string>("PRAGMA quick_check;"));
                if (result != "ok")
                    throw new NotSupportedException($"The database failed the quick_check function with a '{result}' result");

                // make sure syncronization is in NORMAL, https://www.sqlite.org/pragma.html#pragma_synchronous
                Execute(() => conn.ExecuteScalar<int>("PRAGMA synchronous=NORMAL;"));

                // enable Write-Ahead Logging https://www.sqlite.org/wal.html
                Execute(() => conn.ExecuteScalar<int>("PRAGMA journal_mode=WAL;"));

                if (pageSize.HasValue)
                    Execute(() => conn.ExecuteScalar<int>($"PRAGMA page_size={pageSize.Value};"));

                Execute(() => conn.ExecuteScalar<int>("PRAGMA cache_size=5000;"));

                MigrateDatabase(conn);

                Execute(() => conn.ExecuteScalar<int>("PRAGMA wal_checkpoint(RESTART);"));

                Execute(() => conn.ExecuteScalar<int>("VACUUM;"));
                Execute(() => conn.ExecuteScalar<int>("REINDEX;"));
                Execute(() => conn.ExecuteScalar<int>("ANALYZE;"));
            }
            catch (SQLiteException exc) when (exc.Result == Result.Corrupt || exc.Result == Result.NonDBFile)
            {
                DeleteDatabase(conn);

                // intentially crash the application
                throw;
            }
            catch (NotSupportedException)
            {
                DeleteDatabase(conn);

                // intentially crash the application
                throw;
            }

            return new Tuple<SQLiteConnection, SQLiteConnectionPool>(conn, new SQLiteConnectionPool(databasePath, connectionPoolFlags) {
                Trace = Trace,
                TraceTime = TraceTime,
                TraceTimeExceeding = TraceTimeExceeding,
                BusyTimeout = BusyTimeout,
                Tracer = l => Tracer($"Read Connection: {l}")
            });
        }

        protected static void EnsureColumnsMatch<T>(SQLiteConnection conn)
        {
           var version = Execute(() => conn.ExecuteScalar<int>("PRAGMA user_version;"));

            var tableMapping = conn.GetMapping<T>();
            var expectedColumns = tableMapping.Columns.OrderBy(x => x.Name).ToList();
            var expectedIndicies = expectedColumns.SelectMany(c => c.Indices.Select(i => new SQLiteIndex {Name = i.Name, IsUnique = i.Unique})).ToList();

            var actualColumns = conn.GetTableInfo(tableMapping.TableName).OrderBy(x => x.Name).ToDictionary(x => x.Name);
            var actualIndicies = conn.GetTableIndexInfo(tableMapping.TableName).Where(x => x.Origin != "pk").OrderBy(x => x.Name);

            // note: we use a dictionary because you are not allowed to delete columns from sqlite tables
            // so we want to just compare the expected columns from object(T) and leave any others that
            // exist at rest

            foreach (var expected in expectedColumns)
            {
                if (!actualColumns.ContainsKey(expected.Name))
                    throw new NotSupportedException($"The column '{expected.Name}' in table '{tableMapping.TableName}' (v{version}) is missing.");

                var actual = actualColumns[expected.Name];

                if (expected.IsPK != actual.IsPK)
                    throw new NotSupportedException($"The column '{expected.Name}' in table '{tableMapping.TableName}' (v{version}) primary key doesn't match {expected.IsPK} vs {(actual.IsPK)}.");

                if (expected.IsNullable != !actual.IsNotNull)
                    throw new NotSupportedException($"The column '{expected.Name}' in table '{tableMapping.TableName}' (v{version}) nullability doesn't match {expected.IsNullable} vs {!actual.IsNotNull}.");

                if (SQLite3.GetSqlType(expected.ColumnType, false) != actual.ColumnType)
                    throw new NotSupportedException($"The column '{expected.Name}' in table '{tableMapping.TableName}' (v{version}) type doesn't match {SQLite3.GetSqlType(expected.ColumnType, false)} vs {actual.ColumnType}.");
            }

            var missingIndicies = expectedIndicies.Where(a => actualIndicies.Contains(a) == false).ToList();

            if (missingIndicies.Count > 0)
                throw new NotSupportedException($"Missing expected indicies {string.Join(", ", missingIndicies.Select(i => i.Name))}");

            var extraIndicies = actualIndicies.Where(e => expectedIndicies.Contains(e) == false).ToList();

            if (extraIndicies.Count > 0)
                throw new NotSupportedException($"Extra indicies {string.Join(", ", extraIndicies.Select(i => i.Name))}");
        }

        private void MigrateDatabase(SQLiteConnection conn)
        {
            Func<int> getDbVersion = () => Execute(() => conn.ExecuteScalar<int>("PRAGMA user_version;"));
            Action<int> setDbVersion = value => Execute(() => conn.Execute($"PRAGMA user_version={value};"));

            var dbVersion = getDbVersion();
            while (dbVersion < LatestDatabaseVersion)
            {
                MigrateDatabase(dbVersion, action => {
                    Execute(() => Transaction(conn, innerConn => {
                        var migratedDbVersion = action(innerConn);
                        setDbVersion(migratedDbVersion);
                        return migratedDbVersion;
                    }));
                });

                var newDbVersion = getDbVersion();

                if (dbVersion == newDbVersion)
                    throw new NotSupportedException($"The database version {dbVersion} is not supported.");

                dbVersion = newDbVersion;
            }

            EnsureSchemaMatchesAfterMigration(conn);
        }

        protected abstract void MigrateDatabase(int fromVersion, Action<Func<SQLiteConnection, int>> migrate);

        protected abstract void EnsureSchemaMatchesAfterMigration(SQLiteConnection conn);

        #endregion

        ///<summary>Optimize database, executes PRAGMA optimize</summary>
        public void Optimize() => Write(Optimize);

        ///<summary>Optimize database, executes PRAGMA optimize</summary>
        public static int Optimize(SQLiteConnection conn)
        {
            try
            {
                return conn.Execute("PRAGMA optimize;");
            }
            catch (Exception exc)
            {
                Log(SqlLogType.Warning, "Optimize failed", exc);
                return 0;
            }
        }

        ///<summary>Analyze database, executes reindex, then analyze</summary>
        public void Analyze() => Write(Analyze);

        ///<summary>Analyze database, executes reindex, then analyze</summary>
        public static int Analyze(SQLiteConnection conn)
        {
            try
            {
                conn.Execute("reindex;");

                // https://sqlite.org/lang_analyze.html
                return conn.Execute("analyze;");
            }
            catch (Exception exc)
            {
                Log(SqlLogType.Warning, "Analyze failed", exc);
                return 0;
            }
        }

        /// <seealso cref="https://sqlite.org/c3ref/wal_checkpoint_v2.html"/>
        public void Checkpoint()
        {
            try
            {
                using (GetWriteConnectionLock(nameof(Checkpoint)))
                {
                    if (_writeConnection.Handle != SQLiteConnection.NullHandle)
                    {
                        int logSize;
                        int framesCheckPointed;
                        raw.sqlite3_wal_checkpoint_v2(_writeConnection.Handle, null, raw.SQLITE_CHECKPOINT_RESTART, out logSize, out framesCheckPointed);
                        Log(SqlLogType.Debug, String.Format("sqlite3_wal_checkpoint_v2(RESTART) completed, logSize: {0}; framesCheckPointed: {1}", logSize, framesCheckPointed), null);
                    }
                }
            }
            catch (Exception exc)
            {
                Log(SqlLogType.Warning, "sqlite3_wal_checkpoint_v2(RESTART) failed", exc);
            }
        }

        public virtual void Dispose()
        {
            using (GetWriteConnectionLock(nameof(Dispose)))
            {
                // per SQLite, this should be closed prior to closing a connection
                // https://sqlite.org/lang_analyze.html
                Optimize();

                // 1) dispose of the read pool so that it blocks any new read connections
                _readPool?.Dispose();

                // 2) run a RESTART checkpoint which blocks until all readers are finished with the log file
                Checkpoint();

                // 3) dispose of the write connection last, since it is used in the checkpoint
                _writeConnection?.Dispose();

                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            // stop monitoring for startup changing
            _startupChangedSubscription?.Dispose();
        }

#if TEST
        public TResult ExecuteSql<TResult>(string sql, params object[] args)
        {
            return Write(conn => conn.ExecuteScalar<TResult>(sql, args));
        }
#endif

        public static bool DeleteDatabase(SQLiteConnection conn)
        {
            if (conn == null)
                throw new ArgumentNullException(nameof(conn));

            var databasePath = default(string);

            if (conn.Handle != SQLiteConnection.NullHandle)
            {
                // this will be set to null if it is an in memory database
                // the databasePath from the connection should override the
                // path passed in
                databasePath = raw.sqlite3_db_filename(conn.Handle, "main").utf8_to_string();
                conn.Close();
            }

            // probably in memory database
            if (databasePath == null)
                return false;

            return DeleteDatabase(databasePath);
        }

        public static bool DeleteDatabase(DataAccessLayerBase db)
        {
            if (db == null)
                throw new ArgumentNullException(nameof(db));

            using (db.GetWriteConnectionLock(nameof(DeleteDatabase)))
            {
                var databasePath = default(string);
                var conn = db._writeConnection;

                if (conn != null && conn.Handle != SQLiteConnection.NullHandle)
                {
                    // this will be set to null if it is an in memory database
                    // the databasePath from the connection should override the
                    // path passed in
                    databasePath = raw.sqlite3_db_filename(conn.Handle, "main").utf8_to_string();
                }

                // dispose of the database
                db.Dispose();

                // probably in memory database
                if (databasePath == null)
                    return false;

                return DeleteDatabase(databasePath);
            }
        }

        public static bool DeleteDatabase(string databasePath)
        {
            if (databasePath == null)
                throw new ArgumentNullException(nameof(databasePath));

            if (String.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException($"'{nameof(databasePath)}' cannot be empty", nameof(databasePath));

            bool delete (string path) {
                if (File.Exists(path))
                    File.Delete(path);

                return !File.Exists(path);
            };

            var deleted = false;
            deleted |= delete(databasePath);

            try
            {
                deleted |= delete(databasePath + "-journal");
                deleted |= delete(databasePath + "-shm");
                deleted |= delete(databasePath + "-wal");
            }
            // Mainly this is to ignore UnauthorizedAccessException we seem to be getting when deleting the .shm file - these should be cleaned up when we next create a new .db
            catch(SecurityException) {}
            catch(UnauthorizedAccessException) {}

            var prefix = Path.GetFileName(databasePath) + "-mj*";
            var parentPath = Directory.GetParent(databasePath);

            if (parentPath != null)
            {
                var files = parentPath.EnumerateFiles(prefix, SearchOption.TopDirectoryOnly);
                deleted = files.Aggregate(deleted, (current, file) => current | delete(file.FullName));
            }

            return deleted;
        }

        public static bool Transaction(SQLiteConnection conn, Func<SQLiteConnection, bool> func)
        {
            if (conn == null)
                throw new ArgumentNullException(nameof(conn));

            if (func == null)
                throw new ArgumentNullException(nameof(func));

            if (conn.IsInTransaction)
                throw new DatabaseAlreadyInTransaction();

            bool success;

            try
            {
                conn.BeginTransaction();
                success = func(conn);

                if (success)
                    conn.Commit(rollbackOnFailure: false);
                else
                    conn.Rollback();
            }
            catch (Exception)
            {
                conn.Rollback(noThrow: true);
                throw;
            }

            return success;
        }

        public static T Transaction<T>(SQLiteConnection conn, Func<SQLiteConnection, T> func)
        {
            if (conn == null)
                throw new ArgumentNullException(nameof(conn));

            if (func == null)
                throw new ArgumentNullException(nameof(func));

            if (conn.IsInTransaction)
                throw new DatabaseAlreadyInTransaction();

            T ret;

            try
            {
                conn.BeginTransaction();
                ret = func(conn);
                conn.Commit(rollbackOnFailure: false);
            }
            catch (Exception)
            {
                conn.Rollback(noThrow: true);
                throw;
            }

            return ret;
        }

        ///<summary>The default number of retires that will happen when executing a database command.</summary>
        public const int DefaultRetries = 10;
        private static readonly Random _jitter = new Random();

        public static T Execute<T>(Func<T> func, int retries = DefaultRetries)
        {
            if (func == null)
                throw new ArgumentNullException(nameof(func));

            Exception lastExc = null;

            for (var i = 0; i < retries; i++)
            {
                if (i > 0)
                {
                    var millisecondsDelay = _jitter.Next(minValue: 500, maxValue: 5000);
                    Log(SqlLogType.Debug, String.Format("retrying database command for {0}/{1}, backing off for {2} ms", i, retries, millisecondsDelay), null);
                    Task.Delay(millisecondsDelay).Wait();
                }

                try
                {
                    return func();
                }
                catch (DatabaseWriteLockTimeoutException exc)
                {
                    lastExc = exc;
                    Log(SqlLogType.Warning, "database write lock timeout", exc);
                }
                catch (SQLiteException exc) when (exc.Result == Result.Busy || exc.Result.HasFlag(Result.Locked))
                {
                    lastExc = exc;
                    Log(SqlLogType.Warning, "sqlite error from database", exc);
                }
                catch (IndexOutOfRangeException exc)
                {
                    lastExc = exc;
                    Log(SqlLogType.Warning, "error from database", exc);
                }
            }

            throw new DatabaseRetryFailedException($"The call to {func} failed.", lastExc);
        }
        protected T Write<T>(Func<SQLiteConnection, T> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Execute(() => {
            if (func == null)
                throw new ArgumentNullException(nameof(func));

            using (GetWriteConnectionLock(caller))
                return func(_writeConnection);
        }, retries);

        protected T Read<T>(Func<SQLiteConnection, T> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Execute(() => {

            if (func == null)
                throw new ArgumentNullException(nameof(func));

            // when the connection consisted of an in memory database, the savepoint transactions weren't working when cache=shared
            // is enabled, however standard begin.. commit.. rollback.. commands were working, so it must be a problem.
            if (IsInMemoryDatabase)
                using (GetWriteConnectionLock(caller))
                    return func(_writeConnection);

            using (var conn = _readPool.GetConnection())
                return func(conn.Connection);
        }, retries);

        protected T TransactionRead<T>(Func<SQLiteConnection, T> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Read(conn => Transaction(conn, func), retries, caller);

        protected T TransactionWrite<T>(Func<SQLiteConnection, T> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Write(conn => Transaction(conn, func), retries, caller);

        protected bool TransactionWrite(Func<SQLiteConnection, bool> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Write(conn => Transaction(conn, func), retries, caller);

        public static T Attach<T>(DataAccessLayerBase db1, DataAccessLayerBase db2, string db1Alias, string db2Alias, Func<SQLiteConnection, T> func, int retries = DefaultRetries, [CallerMemberName] string caller = null) => Execute(() => {
                using (db1.GetWriteConnectionLock(caller + "|" + db1.GetType().Name))
                using (db2.GetWriteConnectionLock(caller + "|" + db2.GetType().Name))
                using (var conn = new SQLiteConnection(InMemoryDatabase))
                using (new AttachDatabase(conn, db1Alias, db1._writeConnection.DatabasePath))
                using (new AttachDatabase(conn, db2Alias, db2._writeConnection.DatabasePath))
                    return func(conn);
            }, retries);

#pragma warning disable 0618
        private class AttachDatabase : IDisposable
        {
            private readonly SQLiteConnection _conn;
            private readonly string _alias;

            public AttachDatabase(SQLiteConnection conn, string alias, string path)
            {
                _conn = conn;
                _alias = alias;

                var query = $"ATTACH DATABASE ? AS {_alias}";
                _conn.Execute(query, path);
            }

            public void Dispose()
            {
                var db1Query = $"DETACH DATABASE {_alias}";
                _conn.Execute(db1Query);
            }
        }

        private class DatabaseWriteLock : IDisposable
        {
            private readonly DataAccessLayerBase _db;

            private DatabaseWriteLock(DataAccessLayerBase db, string reason)
            {
                _db = db;
                _db._writeConnectionLockReason = reason;
            }

            public static DatabaseWriteLock Lock(DataAccessLayerBase db, string reason, TimeSpan timeout)
            {
                // try to aquire a lock for X timespan, throw a deadlock exception if we cannot aquire
                if (!Monitor.TryEnter(db._writeConnectionLock, timeout))
                {
                    var lockReason = db._writeConnectionLockReason;
                    throw new DatabaseWriteLockTimeoutException($"Could not get a write lock for {db.GetType().Name}, because of this lock: {lockReason}");
                }

                return new DatabaseWriteLock(db, reason);
            }

            public void Dispose()
            {
                if (Monitor.IsEntered(_db._writeConnectionLock))
                {
                    _db._writeConnectionLockReason = null;
                    Monitor.Exit(_db._writeConnectionLock);
                }
            }
        }
#pragma warning restore 0618
    }
}
