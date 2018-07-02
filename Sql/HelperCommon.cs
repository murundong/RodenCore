using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
    public class HelperCommon
    {

        /// <summary>
        /// 获取类的属性
        /// </summary>
        /// <param name="type"></param>
        /// <param name="exclude"></param>
        /// <param name="include"></param>
        /// <returns></returns>
        public static List<PropertyInfo> GetProperty(Type type,string[] exclude=null,string[] include = null)
        {
            var props = type.GetProperties();
            List<PropertyInfo> list = new List<PropertyInfo>();
            foreach (var p in props)
            {
                if(include!=null && include.Contains(p.Name))
                {
                    list.Add(p);
                }
                else if(exclude!=null && exclude.Contains(p.Name))
                {
                    continue;
                }
                else
                {
                    list.Add(p);
                }
            }
            return list;
        }

        /// <summary>
        /// 获取具有某一特性的属性，当Include不为空是，则exclude将无效
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="Exclude">需要排除的属性名</param>
        /// <param name="Include">需要包含的属性名</param>
        /// <returns></returns>
        public static Dictionary<PropertyInfo, T> GetPropertyAttribute<T>(Type type, string[] Exclude = null, string[] Include = null) where T : Attribute
        {
            var props = type.GetProperties();
            Dictionary<PropertyInfo, T> dic = new Dictionary<PropertyInfo, T>();
            foreach (var p in props)
            {
                object[] atts = p.GetCustomAttributes(typeof(T), true);
                if (atts.Length == 1)
                {
                    bool isAdd = false;
                    if (Include != null)
                    {
                        if (Include.Contains(p.Name))
                        {
                            isAdd = true;
                        }
                    }
                    else
                    {
                        if (Exclude != null)
                        {
                            if (!Exclude.Contains(p.Name))
                            {
                                isAdd = true;
                            }
                        }
                        else
                        {
                            isAdd = true;
                        }
                    }
                    if (isAdd)
                    {
                        dic.Add(p, ((T)atts[0]));
                    }
                }
            }
            return dic;
        }
    }
}
