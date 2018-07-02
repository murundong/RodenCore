using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
    /// <summary>
    /// 分页类
    /// </summary>
    public abstract class SqlPageQuery
    {
        public SqlPageQuery()
        {
            PageIndex = 1;
            PageSize = 15;
        }

        /// <summary>
        /// 索引
        /// </summary>
        public int PageIndex { get; set; }

        /// <summary>
        /// 数目
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// 总数
        /// </summary>
        public int TotalCount { get; set; }

        /// <summary>
        /// 排序依据
        /// </summary>
        public string Sort { get; set; }
    }
}
