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
using System.Reflection;
using System.Linq;
using System.Collections.Generic;

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    public class EnumCacheInfo
    {
        private Dictionary<int, object> _enumValues;

        public EnumCacheInfo(Type type)
        {
            var typeInfo = type.GetTypeInfo();

            IsEnum = typeInfo.IsEnum;

            if (IsEnum)
            {
                StoreAsText = typeInfo.CustomAttributes.Any(x => x.AttributeType == typeof(StoreAsTextAttribute));

                var values = Enum.GetValues(type);

                _enumValues = new Dictionary<int, object>(values.Length);

                foreach (object e in values)
                    _enumValues[Convert.ToInt32(e)] = e;
            }
        }

        public bool IsEnum { get; private set; } = false;

        public bool StoreAsText { get; private set; } = false;

        public object GetEnumFromInt32Value(int value)
        {
            var val = _enumValues[value];

			if (StoreAsText)
				return val.ToString();

			return val;
        }
	}

    public static class EnumCache
	{
        private static readonly Dictionary<Type, EnumCacheInfo> Cache = new Dictionary<Type, EnumCacheInfo>();

		public static EnumCacheInfo GetInfo<T> ()
		{
			return GetInfo (typeof (T));
		}

		public static EnumCacheInfo GetInfo (Type type)
		{
			lock (Cache) {
				EnumCacheInfo info = null;
				if (!Cache.TryGetValue (type, out info)) {
					info = new EnumCacheInfo (type);
					Cache[type] = info;
				}

				return info;
			}
		}
	}
}
