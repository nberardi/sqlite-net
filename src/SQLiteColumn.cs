using System;

namespace SQLite
{
    	[Preserve (AllMembers = true)]
		public class SQLiteColumn
		{
            [Column("cid")]
            public int ColumnId { get; set; }

			[Column ("name")]
			public string Name { get; set; }

            [Column ("type")]
            public string ColumnType { get; set; }

            [Column ("notnull")]
            public bool IsNotNull { get; set; }

            [Column("dflt_value")]
            public string DefaultValue { get; set; }

            [Column ("pk")]
            public bool IsPK { get; set; }

            public override string ToString() => $"{Name} ({ColumnType})";
        }
}
