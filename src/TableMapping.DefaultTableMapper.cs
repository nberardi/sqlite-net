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

    public partial class TableMapping
    {
        private class DefaultTableMapper : ITableMapper
        {
            private static readonly Action<PropertyInfo, object, object> _setValue = (prop, obj, val) => prop.SetValue(obj, val, null);
            private static readonly Func<PropertyInfo, object, object> _getValue = (prop, obj) => prop.GetValue(obj, null);

            public List<TableColumn> GetColumns(Type t, CreateFlags createFlags = CreateFlags.None)
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

                var cols = new List<TableColumn>();
                foreach (var p in props)
                {
                    var ignore = p.GetCustomAttributes(typeof(IgnoreAttribute), true).Count() > 0;

                    if (p.CanWrite && !ignore)
                        cols.Add(new TableColumn(p, _setValue, _getValue, createFlags));
                }

                return cols;
            }

            public object CreateInstance(Type t)
            {
                return Activator.CreateInstance(t);
            }

            public Type GetSchema(Type t) => t;
        }
    }
}
