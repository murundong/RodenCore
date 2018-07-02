using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
   


    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
    public class SDAttribute : Attribute
    {
        /// <summary>
        /// 初始化
        /// </summary>
        public SDAttribute()
        {
            Action = SDAction.Insert | SDAction.Update;
        }


        public string Name { get; set; }
      
        /// <summary>
        /// Insert或者Update是否包含该属性
        /// </summary>
        public SDAction Action { get; set; }
    }

}
