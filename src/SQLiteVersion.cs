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

#pragma warning disable 1591 // XML Doc Comments
// ReSharper disable All

namespace SQLite
{
    /// <seealso href="https://sqlite.org/c3ref/c_source_id.html"/>
    public struct SQLiteVersion : IEquatable<SQLiteVersion>, IComparable<SQLiteVersion>, IComparable
    {
        /// <summary>
        /// Indicates whether the two SQLiteVersion instances are equal to each other.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/> if the two instances are equal to each other; otherwise,  <see langword="false"/>.</returns>
        public static bool operator ==(SQLiteVersion x, SQLiteVersion y) => x.Equals(y);

        /// <summary>
        /// Indicates whether the two SQLiteVersion instances are not equal each other.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/> if the two instances are not equal to each other; otherwise,  <see langword="false"/>.</returns>
        public static bool operator !=(SQLiteVersion x, SQLiteVersion y) => !(x == y);

        /// <summary>
        /// Indicates if the the first SQLiteVersion is greater than or equal to the second.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/>if the the first SQLiteVersion is greater than or equal to the second; otherwise, <see langword="false"/>.</returns>
        public static bool operator >=(SQLiteVersion x, SQLiteVersion y) => x._version >= y._version;

        /// <summary>
        /// Indicates if the the first SQLiteVersion is greater than the second.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/>if the the first SQLiteVersion is greater than the second; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(SQLiteVersion x, SQLiteVersion y) => x._version > y._version;

        /// <summary>
        /// Indicates if the the first SQLiteVersion is less than or equal to the second.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/>if the the first SQLiteVersion is less than or equal to the second; otherwise, <see langword="false"/>.</returns>
        public static bool operator <=(SQLiteVersion x, SQLiteVersion y) => x._version <= y._version;

        /// <summary>
        /// Indicates if the the first SQLiteVersion is less than the second.
        /// </summary>
        /// <param name="x">A SQLiteVersion instance.</param>
        /// <param name="y">A SQLiteVersion instance.</param>
        /// <returns><see langword="true"/>if the the first SQLiteVersion is less than the second; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(SQLiteVersion x, SQLiteVersion y) => x._version < y._version;

        private readonly int _version;

        public SQLiteVersion(int version)
        {
            this._version = version;
        }

        /// <summary>
        /// Gets the major version number.
        /// </summary>
        public int Major => _version / 1000000;

        /// <summary>
        /// Gets the minor version number.
        /// </summary>
        public int Minor => (_version / 1000) % 1000;

        /// <summary>
        /// Gets the release version number.
        /// </summary>
        public int Release => _version % 1000;

        /// <summary>
        /// Converts the version number as an integer with the value (Major*1000000 + Minor*1000 + Release).
        /// </summary>
        /// <returns>The version number as an integer</returns>
        public int ToInt32() => _version;

        public override string ToString() => $"{this.Major}.{this.Minor}.{this.Release}";
        public override int GetHashCode() => _version;
        public bool Equals(SQLiteVersion other) => this._version == other._version;
        public override bool Equals(object other) => other is SQLiteVersion && this == (SQLiteVersion)other;
        public int CompareTo(SQLiteVersion other) => this._version.CompareTo(other._version);
        public int CompareTo(object obj)
        {
            if (obj is SQLiteVersion)
            {
                return this.CompareTo((SQLiteVersion)obj);
            }
            throw new ArgumentException("Can only compare to other SQLiteVersion");
        }
    }
}
