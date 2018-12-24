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
using System.Collections.Generic;
using ConcurrentTableDictionary = System.Collections.Concurrent.ConcurrentDictionary<System.Type, SQLite.TableMapping>;
using System.Reflection;
using System.Linq;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    public interface ITableMapper
    {
        List<TableColumn> GetColumns(Type t, CreateFlags createFlags = CreateFlags.None);

        object CreateInstance(Type t);

        Type GetSchema(Type t);
    }

    public partial class TableMapping
    {
        private static readonly ConcurrentTableDictionary _mappings = new ConcurrentTableDictionary();

        public static IEnumerable<TableMapping> TableMappings => _mappings.Values;

        public static void Clear() => _mappings.Clear();

        /// <summary>
        /// Retrieves the mapping that is automatically generated for the given type.
        /// </summary>
        /// <returns>
        /// The mapping represents the schema of the columns of the database and contains
        /// methods to set and get properties of objects.
        /// </returns>
        public static TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            TableMapping map;
            if (!_mappings.TryGetValue(type, out map))
            {
                map = new TableMapping(type, createFlags);
                if (!_mappings.TryAdd(type, map))
                {
                    // concurrent add attempt this add, retreive a fresh copy
                    _mappings.TryGetValue(type, out map);
                }
            }

            return map;
        }

        /// <summary>
        /// Retrieves the mapping that is automatically generated for the given type.
        /// </summary>
        /// <returns>
        /// The mapping represents the schema of the columns of the database and contains
        /// methods to set and get properties of objects.
        /// </returns>
        public static TableMapping GetMapping<T>()
        {
            return GetMapping(typeof(T));
        }

        private readonly ITableMapper _mapper;

        private Type Schema { get; }

        public string TableName { get; }

		public bool WithoutRowId { get; }

        public TableColumn[] Columns { get; }

        public TableColumn PK { get; }

		public string GetByPrimaryKeySql { get; private set; }

		public CreateFlags CreateFlags { get; private set; }

        private readonly TableColumn _autoPk;
        private TableColumn[] _insertColumns;
        private TableColumn[] _insertOrReplaceColumns;
        private Dictionary<string, TableColumn> _columnNameIndex;
        private Dictionary<string, TableColumn> _columnPropertyNameIndex;
        private TableColumn[] _updateColumns;

        private TableMapping(Type type, CreateFlags createFlags = CreateFlags.None)
		{
			CreateFlags = createFlags;

			var typeInfo = type.GetTypeInfo ();
            var mapperAttr = (TableMapperAttribute) typeInfo.GetCustomAttributes(typeof(TableMapperAttribute), true).FirstOrDefault();
            var mapperType = mapperAttr != null ? mapperAttr.Mapper : typeof(DefaultTableMapper);

            _mapper = Activator.CreateInstance(mapperType) as ITableMapper;
            if (_mapper == null)
                throw new Exception($"{mapperType.Name} is not of type {nameof(ITableMapper)}.");

            Schema = _mapper.GetSchema(type);
            var schemaInfo = Schema.GetTypeInfo();

			var tableAttr = schemaInfo.GetCustomAttributes().OfType<TableAttribute>().FirstOrDefault();

            TableName = (tableAttr != null && !string.IsNullOrEmpty(tableAttr.Name)) ? tableAttr.Name : Schema.Name;
			WithoutRowId = tableAttr != null ? tableAttr.WithoutRowId : false;

            Columns = _mapper.GetColumns(type, createFlags).ToArray();
            foreach (var c in Columns)
            {
                if (c.IsAutoInc && c.IsPK)
                {
                    _autoPk = c;
                }
                if (c.IsPK)
                {
                    PK = c;
                }
            }

            HasAutoIncPK = _autoPk != null;
            GetByPrimaryKeySql = PK != null
                ? $"select * from \"{TableName}\" where \"{PK.Name}\" = ?"
                : $"select * from \"{TableName}\" limit 1";
        }

        public List<TableIndex> GetIndexes()
        {
            var indexes = new Dictionary<string, List<IndexedAttribute>>();

            string GetIndexName(string tableName, string columnName, IndexedAttribute attr) {
                return !String.IsNullOrWhiteSpace(attr.Name)
                    ? attr.Name
                    : $"{(attr.Unique ? "UX" : "IX")}_{tableName}_{columnName}";
            };

            foreach (var c in Columns)
            {
                foreach (var i in c.Indices)
                {
                    var indexName = GetIndexName(TableName, c.Name, i);
                    List<IndexedAttribute> list;

                    if (!indexes.TryGetValue(indexName, out list)) {
                        list = new List<IndexedAttribute>();
                        indexes.Add(indexName, list);
                    }

                    i.ColumnName = c.Name;
                    list.Add(i);
                }
            }

            var tableIndexes = new List<TableIndex>(indexes.Count);
            foreach(var i in indexes)
            {
                // don't know how this would happen
                if (i.Value.Count == 0)
                    throw new Exception($"The index {i.Key} doesn't have any columns.");

                var unique = i.Value.First().Unique;
                var anyNotMatchingUnique = i.Value.Where (x => x.Unique != unique).Any();

                if (anyNotMatchingUnique)
                    throw new Exception($"The index {i.Key} needs to have all Unique properties matching on columns.");

                var ix = new TableIndex {
                    IndexName = i.Key,
                    TableName = TableName,
                    Unique = unique,
                    Columns = i.Value.OrderBy(x => x.Order).Select(x => new TableColumnOrder() {
                        ColumnName = x.ColumnName,
                        Direction = x.Direction
                    }).ToList()
                };
                tableIndexes.Add(ix);
            }

            return tableIndexes;
        }

        public bool IsObjectRelated(object obj)
        {
            if (obj == null)
                return false;

            return Schema.IsAssignableFrom(obj.GetType());
        }

        public object CreateInstance() => _mapper.CreateInstance(Schema);

        public bool HasAutoIncPK { get; private set; }

        public void SetAutoIncPK(object obj, long id)
        {
            _autoPk?.SetValue(obj, Convert.ChangeType(id, _autoPk.ColumnType, null));
        }

        public TableColumn[] UpdateColumns => _updateColumns ?? (_updateColumns = Columns.Where(c => !c.IsPK).ToArray());

        public TableColumn[] InsertColumns => _insertColumns ?? (_insertColumns = Columns.Where(c => !c.IsAutoInc).ToArray());

        public TableColumn[] InsertOrReplaceColumns => _insertOrReplaceColumns ?? (_insertOrReplaceColumns = Columns.ToArray());

        private Dictionary<string, TableColumn> ColumnNameIndex => _columnNameIndex ?? (_columnNameIndex = Columns.ToDictionary(x => x.Name));

        private Dictionary<string, TableColumn> ColumnPropertyNameIndex => _columnPropertyNameIndex ?? (_columnPropertyNameIndex = Columns.ToDictionary(x => x.PropertyName));

        public TableColumn FindColumnWithPropertyName(string propertyName)
        {
            TableColumn col = null;
            ColumnPropertyNameIndex.TryGetValue(propertyName, out col);
            return col;
        }

        public TableColumn FindColumn(string columnName)
        {
            TableColumn col = null;
            ColumnNameIndex.TryGetValue(columnName, out col);
            return col;
        }

        public override string ToString() => TableName;
    }
}
