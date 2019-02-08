using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;

namespace SQLite
{
    [Serializable, ExcludeFromCodeCoverage]
    public class DatabaseRetryFailedException : Exception
    {
        public DatabaseRetryFailedException()
        {
        }

        public DatabaseRetryFailedException(string message) : base(message)
        {
        }

        public DatabaseRetryFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected DatabaseRetryFailedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }

    [Serializable, ExcludeFromCodeCoverage]
    public class DatabaseWriteLockTimeoutException : TimeoutException
    {
        public DatabaseWriteLockTimeoutException()
        {
        }

        public DatabaseWriteLockTimeoutException(string message) : base(message)
        {
        }

        public DatabaseWriteLockTimeoutException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected DatabaseWriteLockTimeoutException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }

    [Serializable, ExcludeFromCodeCoverage]
    public class DatabaseAlreadyInTransaction : TimeoutException
    {
        public DatabaseAlreadyInTransaction()
        {
        }

        public DatabaseAlreadyInTransaction(string message) : base(message)
        {
        }

        public DatabaseAlreadyInTransaction(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected DatabaseAlreadyInTransaction(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
