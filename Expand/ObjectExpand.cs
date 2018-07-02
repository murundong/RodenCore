using RodenCore.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Expand
{
    public static class ObjectExpand
    {
        public static string ToSqlString(this object obj)
        {
            return SqlManage.GetSqlString(obj);
        }
    }
}
