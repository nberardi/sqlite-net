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
        List<TableMapping.Column> GetColumns(Type t, CreateFlags createFlags = CreateFlags.None);

        object CreateInstance(Type t);

        Type GetSchema(Type t);
    }

    public class TableMapping
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

        public Column[] Columns { get; }

        public Column PK { get; }

		public string GetByPrimaryKeySql { get; private set; }

		public CreateFlags CreateFlags { get; private set; }

        private readonly Column _autoPk;
        private Column[] _insertColumns;
        private Column[] _insertOrReplaceColumns;
        private Dictionary<string, Column> _columnNameIndex;
        private Dictionary<string, Column> _columnPropertyNameIndex;
        private Column[] _updateColumns;

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

        public List<Index> GetIndexs()
        {
            var indexes = new Dictionary<string, Index>();
            foreach (var c in Columns)
            {
                foreach (var i in c.Indices)
                {
                    var iname = i.Name ?? TableName + "_" + c.Name;
                    Index iinfo;
                    if (!indexes.TryGetValue(iname, out iinfo))
                    {
                        iinfo = new Index {
                            IndexName = iname,
                            TableName = TableName,
                            Unique = i.Unique,
                            Columns = new List<IndexedColumn>()
                        };
                        indexes.Add(iname, iinfo);
                    }

                    if (i.Unique != iinfo.Unique)
                        throw new Exception("All the columns in an index must have the same value for their Unique property");

                    iinfo.Columns.Add(new IndexedColumn {
                        Order = i.Order,
                        ColumnName = c.Name
                    });
                }
            }

            return indexes.Values.ToList();
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

        public Column[] UpdateColumns => _updateColumns ?? (_updateColumns = Columns.Where(c => !c.IsPK).ToArray());

        public Column[] InsertColumns => _insertColumns ?? (_insertColumns = Columns.Where(c => !c.IsAutoInc).ToArray());

        public Column[] InsertOrReplaceColumns => _insertOrReplaceColumns ?? (_insertOrReplaceColumns = Columns.ToArray());

        private Dictionary<string, Column> ColumnNameIndex => _columnNameIndex ?? (_columnNameIndex = Columns.ToDictionary(x => x.Name));

        private Dictionary<string, Column> ColumnPropertyNameIndex => _columnPropertyNameIndex ?? (_columnPropertyNameIndex = Columns.ToDictionary(x => x.PropertyName));

        public Column FindColumnWithPropertyName(string propertyName)
        {
            Column col = null;
            ColumnPropertyNameIndex.TryGetValue(propertyName, out col);
            return col;
        }

        public Column FindColumn(string columnName)
        {
            Column col = null;
            ColumnNameIndex.TryGetValue(columnName, out col);
            return col;
        }

        public override string ToString() => TableName;

        private class DefaultTableMapper : ITableMapper
        {
            private static readonly Action<PropertyInfo, object, object> _setValue = (prop, obj, val) => prop.SetValue(obj, val, null);
            private static readonly Func<PropertyInfo, object, object> _getValue = (prop, obj) => prop.GetValue(obj, null);

            public List<TableMapping.Column> GetColumns(Type t, CreateFlags createFlags = CreateFlags.None)
            {
                var props = new List<PropertyInfo>();
                var baseType = GetSchema(t);
                var propNames = new HashSet<string>();
                while (baseType != typeof(object))
                {
                    var ti = baseType.GetTypeInfo();
                    var newProps = (
                        from p in ti.DeclaredProperties
                        where
                        !propNames.Contains(p.Name) &&
                        p.CanRead && p.CanWrite &&
                        (p.GetMethod != null) && (p.SetMethod != null) &&
                        (p.GetMethod.IsPublic && p.SetMethod.IsPublic) &&
                        (!p.GetMethod.IsStatic) && (!p.SetMethod.IsStatic)
                        select p).ToList();
                    foreach (var p in newProps)
                    {
                        propNames.Add(p.Name);
                    }
                    props.AddRange(newProps);
                    baseType = ti.BaseType;
                }

                var cols = new List<Column>();
                foreach (var p in props)
                {
                    var ignore = p.GetCustomAttributes(typeof(IgnoreAttribute), true).Count() > 0;

                    if (p.CanWrite && !ignore)
                        cols.Add(new Column(p, _setValue, _getValue, createFlags));
                }

                return cols;
            }

            public object CreateInstance(Type t)
            {
                return Activator.CreateInstance(t);
            }

            public Type GetSchema(Type t) => t;
        }

		public class Column
		{
            private readonly PropertyInfo _prop;
            private readonly Action<PropertyInfo, object, object> _setValue;
            private readonly Func<PropertyInfo, object, object> _getValue;

            public string Name { get; }

            public string PropertyName { get; }

            /// <summary>
            /// The difference between this and <see cref="ColumnType"/> is that this doesn't unwrap nullable value types.
            /// </summary>
            public Type PropertyType { get; }

            public object PropertyDefaultValue { get; }

            public Type ColumnType { get; }

            public string Collation { get; }

            public bool IsEnum { get; }

            public bool IsAutoInc { get; }

            public bool IsAutoGuid { get; }

            public bool IsPK { get; }

            public bool IsUnique => IsPK || Indices.Any(x => x.Unique);

            public IEnumerable<IndexedAttribute> Indices { get; }

            public bool IsNullable { get; }

            public int? MaxStringLength { get; }

            public object DefaultValue { get; private set; }

            public bool HasDefaultValue => DefaultValue != null;

            public bool StoreAsText { get; }

            public Column(PropertyInfo prop, Action<PropertyInfo, object, object> setValue, Func<PropertyInfo, object, object> getValue, CreateFlags createFlags)
			{
                const string ImplicitPkName = "Id";
                const string ImplicitIndexSuffix = "Id";

                var colAttr = (ColumnAttribute) prop.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();

                _prop = prop;
                PropertyName = prop.Name;
                PropertyType = prop.PropertyType;

                var propertyTypeInfo = PropertyType.GetTypeInfo();
                PropertyDefaultValue = (PropertyType != null && propertyTypeInfo.IsValueType && Nullable.GetUnderlyingType(PropertyType) == null) ? Activator.CreateInstance(PropertyType) : null;

                _setValue = setValue;
                _getValue = getValue;

                Name = colAttr == null ? prop.Name : colAttr.Name;
                //If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
                ColumnType = Nullable.GetUnderlyingType(PropertyType) ?? PropertyType;

                var columnTypeInfo = ColumnType.GetTypeInfo();
                IsEnum = columnTypeInfo.IsEnum;

                var attr = prop.GetCustomAttributes(true);

                Collation = attr.OfType<CollationAttribute>().FirstOrDefault()?.Value ?? "";

                IsPK = attr.OfType<PrimaryKeyAttribute>().Any() || (createFlags.HasFlag(CreateFlags.ImplicitPK) && String.Equals(prop.Name, ImplicitPkName, StringComparison.OrdinalIgnoreCase));

				var isAuto = attr.OfType<AutoIncrementAttribute>().Any() || (IsPK && createFlags.HasFlag(CreateFlags.AutoIncPK));
				IsAutoGuid = isAuto && ColumnType == typeof (Guid);
				IsAutoInc = isAuto && !IsAutoGuid;

				Indices = attr.OfType<IndexedAttribute>();

				if (!Indices.Any ()
					&& !IsPK
					&& createFlags.HasFlag(CreateFlags.ImplicitIndex)
					&& Name.EndsWith (ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)
					) {
					Indices = new IndexedAttribute[] { new IndexedAttribute () };
				}
				IsNullable = !(IsPK || attr.OfType<NotNullAttribute>().Any());

				MaxStringLength = attr.OfType<MaxLengthAttribute>().FirstOrDefault()?.Value;
                DefaultValue = attr.OfType<DefaultAttribute>().FirstOrDefault()?.Value;
				StoreAsText = attr.OfType<StoreAsTextAttribute>().Any();
			}

            public void SetValue(object obj, object val) => _setValue(_prop, obj, val);

            public object GetValue(object obj) => _getValue(_prop, obj);

            public override string ToString() => $"{Name} ({ColumnType.Name})";
        }

        public class IndexedColumn
        {
            public int Order { get; set; }
            public string ColumnName { get; set; }
        }

        public class Index
        {
            public string IndexName { get; set; }
            public string TableName { get; set; }
            public bool Unique { get; set; }
            public List<IndexedColumn> Columns { get; set; }
        }
    }
}
