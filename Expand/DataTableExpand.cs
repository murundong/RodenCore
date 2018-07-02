using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Expand
{
    public static class DataTableExpand
    {

        /// <summary>
        /// datatable转实体类
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dt"></param>
        /// <param name="MemberFormat">序列化成功后，自定义成员format格式，返回类型必须和原属性类型一致</param>
        /// <returns></returns>
        public static List<T> ConvertTo<T>(this DataTable dt, Func<MemberInfo, object, object> MemberFormat = null)
        {
            List<T> list = new List<T>();
            if (dt == null)
            {
                return new List<T>();
            }
            return JsonConvert.SerializeObject(dt).ConvertTo<List<T>>(MemberFormat);
        }

        /// <summary>
        /// datatable转实体类
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="type"></param>
        /// <param name="MemberFormat"></param>
        /// <returns></returns>
        public static List<T> ConvertTo<T>(this DataTable dt, Type type, Func<MemberInfo, object, object> MemberFormat = null)
        {
            if (dt == null)
            {
                return null;
            }
            return JsonConvert.SerializeObject(dt).ConvertTo<T>(type, MemberFormat);
        }

        /// <summary>
        /// DataRow[]转实体类
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rows"></param>
        /// <param name="MemberFormat">序列化成功后，自定义成员format格式，返回类型必须和原属性类型一致</param>
        /// <returns></returns>
        public static List<T> ConvertTo<T>(this DataRow[] rows, Func<MemberInfo, object, object> MemberFormat = null)
        {
            if (rows == null || rows.Length == 0)
            {
                return new List<T>();
            }
            DataTable dt = rows[0].Table.Clone();
            foreach (var item in rows)
            {
                dt.Rows.Add(item.ItemArray);
            }
            return dt.ConvertTo<T>(MemberFormat);
        }
        /// <summary>
        /// DataRow[]转实体类
        /// </summary>
        /// <param name="rows"></param>
        /// <param name="type"></param>
        /// <param name="MemberFormat"></param>
        /// <returns></returns>
        public static List<T> ConvertTo<T>(this DataRow[] rows, Type type, Func<MemberInfo, object, object> MemberFormat = null)
        {
            if (rows == null || rows.Length == 0)
            {
                return null;
            }
            DataTable dt = rows[0].Table.Clone();
            foreach (var item in rows)
            {
                dt.Rows.Add(item.ItemArray);
            }
            return dt.ConvertTo<T>(type, MemberFormat);
        }
        /// <summary>
        /// DataRow转实体类
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="row"></param>
        /// <param name="MemberFormat">序列化成功后，自定义成员format格式，返回类型必须和原属性类型一致</param>
        /// <returns></returns>
        public static T ConvertTo<T>(this DataRow row, Func<MemberInfo, object, object> MemberFormat = null)
        {
            return new DataRow[] { row }.ConvertTo<T>(MemberFormat).FirstOrDefault();
        }
        /// <summary>
        /// DataRow转实体类
        /// </summary>
        /// <param name="row"></param>
        /// <param name="type"></param>
        /// <param name="MemberFormat"></param>
        /// <returns></returns>
        public static T ConvertTo<T>(this DataRow row, Type type, Func<MemberInfo, object, object> MemberFormat = null)
        {
            return new DataRow[] { row }.ConvertTo<T>(type, MemberFormat).FirstOrDefault();
        }

        /// <summary>
        /// json转实体类
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <param name="MemberFormat">序列化成功后，自定义成员format格式，返回类型必须和原属性类型一致</param>
        /// <returns></returns>
        public static T ConvertTo<T>(this string json, Func<MemberInfo, object, object> MemberFormat = null)
        {
            JsonSerializerSettings jsetting = new JsonSerializerSettings();
            jsetting.NullValueHandling = NullValueHandling.Ignore;
            if (MemberFormat != null)
            {
                jsetting.ContractResolver = new SpecialContractResolver()
                {
                    MemberFormat = MemberFormat
                };
            }
            return JsonConvert.DeserializeObject<T>(json, jsetting);
        }
        /// <summary>
        /// json转实体类
        /// </summary>
        /// <param name="json"></param>
        /// <param name="type"></param>
        /// <param name="MemberFormat"></param>
        /// <returns></returns>
        public static List<T> ConvertTo<T>(this string json, Type type, Func<MemberInfo, object, object> MemberFormat = null)
        {
            JsonSerializerSettings jsetting = new JsonSerializerSettings();
            jsetting.NullValueHandling = NullValueHandling.Ignore;
            if (MemberFormat != null)
            {
                jsetting.ContractResolver = new SpecialContractResolver()
                {
                    MemberFormat = MemberFormat
                };
            }
            var rs = JsonConvert.DeserializeObject(json, type, jsetting) as IEnumerable<T>;
            if (rs == null)
            {
                return null;
            }
            return rs.Cast<T>().ToList();
        }

        public static DataTable ToTable(this IEnumerable obj)
        {
            return JsonConvert.DeserializeObject<DataTable>(JsonConvert.SerializeObject(obj));
        }
    }

    internal class SpecialContractResolver : DefaultContractResolver
    {
        public Func<MemberInfo, object, object> MemberFormat { get; set; }

        protected override IValueProvider CreateMemberValueProvider(MemberInfo member)
        {
            return new SpecialValueProvider(member) { MemberFormat = MemberFormat };
        }
    }


    internal class SpecialValueProvider : IValueProvider
    {
        private readonly DynamicValueProvider _dvp;
        public Func<MemberInfo, object, object> MemberFormat { get; set; }
        public MemberInfo MemberInfo { get; private set; }
        public SpecialValueProvider(MemberInfo memberInfo)
        {
            MemberInfo = memberInfo;
            _dvp = new DynamicValueProvider(memberInfo);
        }

        public void SetValue(object target, object value)
        {
            _dvp.SetValue(target, MemberFormat == null ? value : MemberFormat(MemberInfo, value));
        }

        public object GetValue(object target)
        {
            return _dvp.GetValue(target);
        }
    }
}
