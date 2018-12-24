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
using System.Reflection;
using System.Linq;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    public class TableColumn
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

        public TableColumn(PropertyInfo prop, Action<PropertyInfo, object, object> setValue, Func<PropertyInfo, object, object> getValue, CreateFlags createFlags)
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
}
