using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
    [Flags]
    public enum SDAction
    {
        /// <summary>
        /// 插入
        /// </summary>
        Insert = 1,
        /// <summary>
        /// 更新
        /// </summary>
        Update = 2,
    }

    /// <summary>
    /// 数据库类型
    /// </summary>
    public enum ConnectionType
    {
        MsSql,
        MySql,
        Access
    }

    internal enum SqlDbType
    {
        Binary = 1,
        Byte = 2,
        Boolean = 3,
        Currency = 4,
        DateTime = 6,
        Decimal = 7,
        Double = 8,
        Guid = 9,
        Int16 = 10,
        Int32 = 11,
        Int64 = 12,
        Object = 13,
        SByte = 14,
        Single = 15,
        String = 16,
        Time = 17,
        StringFixedLength = 23,
        DateTimeOffset = 27,
        Enumerable = -1,
    }
}
