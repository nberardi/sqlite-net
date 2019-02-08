using System;
using System.Diagnostics.CodeAnalysis;

namespace SQLite
{
    [Preserve (AllMembers = true)]
    public class SQLiteIndex : IEquatable<SQLiteIndex>
    {
        [Column("seq")]
        public int SequenceOrder { get; set; }

        [Column("name")]
        public string Name { get; set; }

        [Column("unique")]
        public bool IsUnique { get; set; }

        [Column("origin")]
        public string Origin { get; set; }

        [Column("partial")]
        public bool IsPartial { get; set; }

        public bool Equals(SQLiteIndex other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) && IsUnique == other.IsUnique;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((SQLiteIndex) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Name?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ IsUnique.GetHashCode();
                return hashCode;
            }
        }
    }

    [Preserve (AllMembers = true)]
    public class SQLiteIndexColumn : IEquatable<SQLiteIndexColumn>
    {
        [Column("seqno")]
        public int SequenceOrder { get; set; }

        [Column("name")]
        public string Name { get; set; }

        [Column("cid")]
        public int ColumnId { get; set; }

        public bool Equals(SQLiteIndexColumn other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) && ColumnId == other.ColumnId;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((SQLiteIndexColumn) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Name?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ ColumnId.GetHashCode();
                return hashCode;
            }
        }
    }
}
