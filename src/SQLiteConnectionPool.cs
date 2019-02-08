using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using SQLite;

namespace SQLite
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class SQLiteConnectionPool : IDisposable
    {
        private readonly Queue<SQLiteConnection> _freeConnections = new Queue<SQLiteConnection>();
        private readonly HashSet<SQLiteConnection> _usedConnections = new HashSet<SQLiteConnection>();
        private readonly object _lock = new object();
        private readonly AutoResetEvent _connectionRelease = new AutoResetEvent(true);
        private bool _disposed;

        public SQLiteConnectionPool(string databasePath, SQLiteOpenFlags flags = SQLiteOpenFlags.ReadOnly, int minPoolSize = 1, int maxPoolSize = 10)
        {
            if (minPoolSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(minPoolSize), minPoolSize, "minPoolSize must be 1 or greater.");

            if (maxPoolSize <= 0 || maxPoolSize < minPoolSize)
                throw new ArgumentOutOfRangeException(nameof(maxPoolSize), maxPoolSize, "maxPoolSize must be greater than 1 and minPoolSize");

            DatabasePath = databasePath;
            DatabaseOpenFlags = flags;

            MinPoolSize = minPoolSize;
            MaxPoolSize = maxPoolSize;
        }

        private bool _trace = false;
        private bool _traceTime = false;
        private TimeSpan _traceTimeExceeding = TimeSpan.FromMilliseconds(500);
        private TimeSpan _busyTimeout = TimeSpan.FromSeconds(1);
        private Action<string> _tracer;

		/// <summary>
		/// Whether to writer queries to <see cref="Tracer"/> during execution.
		/// </summary>
        public bool Trace
        {
            get { return _trace; }
            set
            {
                _trace = value;
                lock (_lock)
                {
                    foreach (var c in _freeConnections)
                        c.Trace = _trace;

                    foreach (var c in _usedConnections)
                        c.Trace = _trace;
                }
            }
        }

        /// <summary>
        /// Whether Trace lines should be written that show the execution time of queries.
        /// </summary>
        public bool TraceTime
        {
            get { return _traceTime; }
            set
            {
                _traceTime = value;
                lock (_lock)
                {
                    foreach (var c in _freeConnections)
                        c.TraceTime = _traceTime;

                    foreach (var c in _usedConnections)
                        c.TraceTime = _traceTime;
                }
            }
        }

        /// <summary>
        /// Write a log message when the time exceeds a defined threshold.
        /// </summary>
        public TimeSpan TraceTimeExceeding
        {
            get { return _traceTimeExceeding; }
            set
            {
                _traceTimeExceeding = value;
                lock (_lock)
                {
                    foreach (var c in _freeConnections)
                        c.TraceTimeExceeding = _traceTimeExceeding;

                    foreach (var c in _usedConnections)
                        c.TraceTimeExceeding = _traceTimeExceeding;
                }
            }
        }

        /// <summary>
		/// The delegate responsible for writing trace lines.
		/// </summary>
		/// <value>The tracer.</value>
		public Action<string> Tracer
        {
            get { return _tracer; }
            set
            {
                _tracer = value;
                lock (_lock)
                {
                    foreach (var c in _freeConnections)
                        c.Tracer = _tracer;

                    foreach (var c in _usedConnections)
                        c.Tracer = _tracer;
                }
            }
        }

		/// <summary>
		/// Sets a busy handler to sleep the specified amount of time when a table is locked.
		/// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
		/// </summary>
        public TimeSpan BusyTimeout
        {
            get { return _busyTimeout; }
            set
            {
                _busyTimeout = value;
                lock (_lock)
                {
                    foreach (var c in _freeConnections)
                        c.BusyTimeout = _busyTimeout;

                    foreach (var c in _usedConnections)
                        c.BusyTimeout = _busyTimeout;
                }
            }
        }

        public string DatabasePath { get; }
        public SQLiteOpenFlags DatabaseOpenFlags { get; }
        public int MinPoolSize { get; }
        public int MaxPoolSize { get; }

        public void Dispose()
        {
            if (_disposed)
                return;

            lock (_lock)
            {
                _disposed = true;

                var freeConnections = _freeConnections.ToArray();
                _freeConnections.Clear();

                foreach (var c in freeConnections)
                    c?.Dispose();

                // the used connections will be cleaned up as they are released
            }
        }

        public SQLiteConnectionPoolConnection GetConnection()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SQLiteConnection));

            SQLiteConnection conn = null;

            try
            {
                Monitor.Enter(_lock);

                if (_freeConnections.Count > 0)
                {
                    conn = _freeConnections.Dequeue();
                    _usedConnections.Add(conn);
                }
                else if (_freeConnections.Count + _usedConnections.Count >= MaxPoolSize)
                {
                    Monitor.Exit(_lock);
                    _connectionRelease.WaitOne();
                    return GetConnection();
                }
                else
                {
                    conn = new SQLiteConnection(DatabasePath, DatabaseOpenFlags)
                    {
                        Trace = Trace,
                        TraceTime = TraceTime,
                        TraceTimeExceeding = TraceTimeExceeding,
                        BusyTimeout = BusyTimeout,
                        Tracer = Tracer
                    };

                    _usedConnections.Add(conn);
                }
            }
            finally
            {
                if (Monitor.IsEntered(_lock))
                    Monitor.Exit(_lock);
            }

            return new SQLiteConnectionPoolConnection(conn, this);
        }

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private bool ReleaseConnection(SQLiteConnection connection)
        {
            if (_disposed)
            {
                connection?.Dispose();
                return true;
            }

            lock (_lock)
            {
                _usedConnections.Remove(connection);

                if (!IsOpen(connection))
                    return false;

                _freeConnections.Enqueue(connection);
                _connectionRelease.Set();
            }

            return true;
        }

        /// <summary>
        ///     Determines whether the connection is alive.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <returns>True if alive; otherwise false.</returns>
        private bool IsOpen(SQLiteConnection connection) => connection.Handle != SQLiteConnection.NullHandle;

        public class SQLiteConnectionPoolConnection : IDisposable
        {
            private readonly SQLiteConnectionPool _pool;

            public SQLiteConnectionPoolConnection(SQLiteConnection conn, SQLiteConnectionPool pool)
            {
                _pool = pool;
                Connection = conn;
            }

            public SQLiteConnection Connection { get; }

            public void Dispose()
            {
                _pool.ReleaseConnection(Connection);
            }
        }
    }
}
