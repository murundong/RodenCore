using RodenCore.Expand;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
    internal class SqlManage
    {
        private static Dictionary<Type, SqlDbType> SqlMap = new Dictionary<Type, SqlDbType>();
        static SqlManage()
        {
            SqlMap[typeof(byte)] = SqlDbType.Byte;
            SqlMap[typeof(sbyte)] = SqlDbType.SByte;
            SqlMap[typeof(short)] = SqlDbType.Int16;
            SqlMap[typeof(ushort)] = SqlDbType.Int16;
            SqlMap[typeof(int)] = SqlDbType.Int32;
            SqlMap[typeof(uint)] = SqlDbType.Int32;
            SqlMap[typeof(long)] = SqlDbType.Int64;
            SqlMap[typeof(ulong)] = SqlDbType.Int64;
            SqlMap[typeof(float)] = SqlDbType.Single;
            SqlMap[typeof(double)] = SqlDbType.Double;
            SqlMap[typeof(decimal)] = SqlDbType.Decimal;
            SqlMap[typeof(bool)] = SqlDbType.Boolean;
            SqlMap[typeof(string)] = SqlDbType.String;
            SqlMap[typeof(char)] = SqlDbType.StringFixedLength;
            SqlMap[typeof(Guid)] = SqlDbType.Guid;
            SqlMap[typeof(DateTime)] = SqlDbType.DateTime;
            SqlMap[typeof(DateTimeOffset)] = SqlDbType.DateTimeOffset;
            SqlMap[typeof(TimeSpan)] = SqlDbType.Time;
            SqlMap[typeof(byte[])] = SqlDbType.Binary;
            SqlMap[typeof(byte?)] = SqlDbType.Byte;
            SqlMap[typeof(sbyte?)] = SqlDbType.SByte;
            SqlMap[typeof(short?)] = SqlDbType.Int16;
            SqlMap[typeof(ushort?)] = SqlDbType.Int16;
            SqlMap[typeof(int?)] = SqlDbType.Int32;
            SqlMap[typeof(uint?)] = SqlDbType.Int32;
            SqlMap[typeof(long?)] = SqlDbType.Int64;
            SqlMap[typeof(ulong?)] = SqlDbType.Int64;
            SqlMap[typeof(float?)] = SqlDbType.Single;
            SqlMap[typeof(double?)] = SqlDbType.Double;
            SqlMap[typeof(decimal?)] = SqlDbType.Decimal;
            SqlMap[typeof(bool?)] = SqlDbType.Boolean;
            SqlMap[typeof(char?)] = SqlDbType.StringFixedLength;
            SqlMap[typeof(Guid?)] = SqlDbType.Guid;
            SqlMap[typeof(DateTime?)] = SqlDbType.DateTime;
            SqlMap[typeof(DateTimeOffset?)] = SqlDbType.DateTimeOffset;
            SqlMap[typeof(TimeSpan?)] = SqlDbType.Time;
            SqlMap[typeof(object)] = SqlDbType.Object;

        }

        #region internal
        public static SqlDbType GetSqlDbType(object obj)
        {
            if (obj == null || obj is DBNull)
            {
                return SqlDbType.Object;
            }
            Type type = obj.GetType();
            SqlDbType rs;
            var nullUnderlyingType = Nullable.GetUnderlyingType(type);
            if (nullUnderlyingType != null) type = nullUnderlyingType;
            if (type.IsEnum && !SqlMap.ContainsKey(type))
            {
                return SqlDbType.Int32;
            }
            if (SqlMap.TryGetValue(type, out rs))
            {
                return rs;
            }
            if (typeof(IEnumerable).IsAssignableFrom(type))
            {
                return SqlDbType.Enumerable;
            }
            return SqlDbType.Object;
        }

        static Type TypeNameLower<T>(T obj)
        {
            Type t;
            if (obj == null)
                t = typeof(T);
            else
                t = obj.GetType();
            return t;
        }


        public static string GetSqlString(object obj)
        {
            string rs = "NULL";
            if (obj != null)
            {
                if (obj.GetType().IsEnum)
                {
                    //
                    return ((int)obj).ToString();
                }
                SqlDbType type = GetSqlDbType(obj);
                switch (type)
                {
                    case SqlDbType.Binary:
                        break;
                    case SqlDbType.Byte:
                        break;
                    case SqlDbType.Boolean:
                        rs = (bool)obj ? "1" : "0";
                        break;
                    case SqlDbType.DateTime:
                        rs = "'" + ((DateTime)obj).ToString("yyyy-MM-dd HH:mm:ss") + "'";
                        break;
                    case SqlDbType.Guid:
                    case SqlDbType.String:
                        rs = "'" + obj.ToString().Replace("'", "''") + "'";
                        break;
                    case SqlDbType.Decimal:
                    case SqlDbType.Double:
                    case SqlDbType.Int16:
                    case SqlDbType.Int32:
                    case SqlDbType.Int64:
                    case SqlDbType.Currency:
                    case SqlDbType.Single:
                        rs = obj.ToString();
                        break;
                    case SqlDbType.Object:
                        break;
                    case SqlDbType.SByte:
                        break;
                    case SqlDbType.Time:
                        break;
                    case SqlDbType.StringFixedLength:
                        break;
                    case SqlDbType.DateTimeOffset:
                        break;
                    case SqlDbType.Enumerable:
                        break;
                    default:
                        break;
                }
            }
            return rs;
        }
        #endregion
    }
}
