using RodenCore.Expand;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace RodenCore.Sql
{
    public class SqlCommand
    {
        static Regex paramReg = new Regex("(@[a-zA-Z0-9_]+)", RegexOptions.IgnoreCase | RegexOptions.ExplicitCapture | RegexOptions.RightToLeft);

        /// <summary>
        /// 语句
        /// </summary>
        public string CommandText { get; set; }

        /// <summary>
        /// 参数
        /// </summary>
        public object[] Params { get; set; }

        /// <summary>
        /// 初始化
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="ps"></param>
        public SqlCommand(string cmdText, params object[] ps)
        {
            this.CommandText = cmdText;
            List<object> list = new List<object>();
            if (ps != null && ps.Count() > 0)
            {
                foreach (var item in ps)
                {
                    if (item == null)
                    {
                        continue;
                    }
                    if (item is IEnumerable<object>)
                    {
                        list.AddRange((IEnumerable<object>)item);
                    }
                    else
                    {
                        list.Add(item);
                    }
                }
            }
            this.Params = list.ToArray();
        }

        private Dictionary<string, object> DefinitionParams()
        {
            var dicParams = new Dictionary<string, object>();
            var matchs = paramReg.Matches(this.CommandText);
            Dictionary<string, Match> paramList = new Dictionary<string, Match>();
            foreach (Match item in matchs)
            {
                string key = item.Value.ToUpper();
                paramList.Add(key, item);
            }

            if (this.Params != null && !string.IsNullOrEmpty(this.CommandText))
            {
                int index = -1;
                foreach (var pm in this.Params)
                {
                    index++;
                    if (pm == null)
                    {
                        continue;
                    }
                    if (pm is IDictionary)
                    {
                        var pps = (IDictionary)pm;
                        foreach (var key in pps.Keys)
                        {
                            string n1 = "@" + key;
                            var value = pps[key] ?? DBNull.Value;
                            if (paramList.Keys.Contains(n1.ToUpper()))
                            {
                                dicParams[n1] = value;
                            }
                            string n2 = "@" + index.ToString() + SqlHelper.specialChar + key;
                            if (paramList.Keys.Contains(n2.ToUpper()))
                            {
                                dicParams[n2] = value;
                            }
                        }
                    }
                    else
                    {
                        Type type = pm.GetType();
                        var pps = HelperCommon.GetProperty(type);
                        foreach (var pp in pps)
                        {
                            string n1 = "@" + pp.Name;
                            var value = pp.GetValue(pm) ?? DBNull.Value;
                            if (paramList.Keys.Contains(n1.ToUpper()))
                            {
                                if (value.GetType().IsArray)
                                {
                                    var vv = (IEnumerable)value;
                                    int m = -1;
                                    List<string> names = new List<string>();
                                    foreach (var v in vv)
                                    {
                                        m++;
                                        string name = n1 + SqlHelper.specialChar + m;
                                        dicParams[name] = v;
                                        names.Add(name);
                                    }
                                    var match = paramList[n1.ToUpper()];
                                    this.CommandText = this.CommandText.Replace(match.Index, match.Length, string.Join(",", names));
                                }
                                else
                                {
                                    dicParams[n1] = value;
                                }
                            }
                            string n2 = "@" + index.ToString() + SqlHelper.specialChar + pp.Name;
                            if (paramList.Keys.Contains(n2.ToUpper()))
                            {
                                if (value.GetType().IsArray)
                                {
                                    var vv = (IEnumerable)value;
                                    int m = -1;
                                    List<string> names = new List<string>();
                                    foreach (var v in vv)
                                    {
                                        m++;
                                        string name = n2 + SqlHelper.specialChar + m;
                                        dicParams[name] = v;
                                        names.Add(name);
                                    }
                                    var match = paramList[n2.ToUpper()];
                                    this.CommandText = this.CommandText.Replace(match.Index, match.Length, string.Join(",", names));
                                }
                                else
                                {
                                    dicParams[n2] = value;
                                }
                            }
                        }
                    }


                }

            }
            return dicParams;
        }

        internal IDbCommand CreateDbCommand(IDbConnection conn)
        {
            var ps = DefinitionParams();
            var cmd = conn.CreateCommand();
            cmd.CommandText = this.CommandText;
            foreach (var item in ps)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = item.Key;
                param.Value = item.Value;
                cmd.Parameters.Add(param);
            }
            return cmd;
        }

        #region Insert
        public static SqlCommand Insert(string table, object tableParam, string[] exclude = null, string[] include = null)
        {
            var ps = HelperCommon.GetProperty(tableParam.GetType(), exclude, include).Select(s => s.Name);
            string sql = $@"Insert into {table} ({string.Join(",", ps)}) values({string.Join(",", ps.Select(s => $"@{s}"))})";
            return new SqlCommand(sql, tableParam);
        }

        public static List<SqlCommand> Insert(string table, IEnumerable arrayParam, string[] Exclude = null, string[] Include = null)
        {
            int maxParamSize = 2099;
            List<SqlCommand> rs = new List<SqlCommand>();
            Dictionary<string, List<object>> dic = new Dictionary<string, List<object>>();
            foreach (var item in arrayParam)
            {
                if (item == null)
                {
                    continue;
                }
                var ps = HelperCommon.GetProperty(item.GetType(), Exclude, Include).Select(p => p.Name);
                if (ps.Count() == 0)
                {
                    continue;
                }
                string key = string.Join(",", ps);
                if (!dic.ContainsKey(key))
                {
                    dic.Add(key, new List<object>());
                }
                dic[key].Add(item);
            }
            foreach (var item in dic)
            {
                var col = item.Key;
                var list = item.Value;
                var ps = col.Split(',');

                int batchSize = (int)Math.Floor(maxParamSize * 1.0 / ps.Length);

                while (list.Count > 0)
                {
                    int index = -1;
                    var arr = list.Take(batchSize);

                    string Sql = $@"insert into {table} ({col}) values({string.Join(",", arr.Select(s => { index++; return $"({string.Join(",", ps.Select(m => $"@{index.ToString() + SqlHelper.specialChar + m}"))})"; }))})";
                    rs.Add(new SqlCommand(Sql, arr.ToArray()));
                    list.RemoveRange(0, (new int[] { list.Count, batchSize }).Min());
                }

            }
            return rs;
        }

        public static SqlCommand Insert<T>(T tableParam, string[] Exclude = null, string[] Include = null)
        {
            var ps = HelperCommon.GetProperty(typeof(T), Exclude, Include).Select(p => p.Name);
            string Sql = $@"insert into {typeof(T).Name} ({string.Join(",", ps)}) values({string.Join(",", ps.Select(s => $"@{s}"))})";
            return new SqlCommand(Sql, tableParam);
        }

        public static SqlCommand Insert<T, TAtt>(T tableParam, string[] Exclude = null, string[] Include = null) where TAtt : Attribute
        {
            Dictionary<PropertyInfo, TAtt> dic = HelperCommon.GetPropertyAttribute<TAtt>(typeof(T), Exclude, Include);
            if (dic.Count == 0)
            {
                return null;
            }
            List<string> ps;
            if (typeof(TAtt) == typeof(SDAttribute))
            {
                ps = dic.Where(p => ((p.Value as SDAttribute).Action & SDAction.Insert) == SDAction.Insert).Select(p => p.Key.Name).ToList();
            }
            else
            {
                ps = dic.Keys.Select(p => p.Name).ToList();
            }

            string Sql = $@"insert into {typeof(T).Name} ({string.Join(",", ps)}) values ({string.Join(",", ps.Select(s => $"@{s}"))})";
            return new SqlCommand(Sql, tableParam);
        }
        #endregion
        #region update
        public static SqlCommand Update(string table, object tableParam, object whereParam, string[] Exclude = null, string[] Include = null)
        {
            return Update(table, tableParam, null, whereParam, Exclude, Include);
        }
        public static SqlCommand Update(string table, object tableParam, string where, object whereParam, string[] Exclude = null, string[] Include = null)
        {
            var pps = HelperCommon.GetProperty(tableParam.GetType(), Exclude, Include).Select(p => p.Name);
            var wps = HelperCommon.GetProperty(whereParam.GetType()).Select(p => p.Name);

            if (string.IsNullOrEmpty(where)) where = string.Join(",", wps.Select(p => p + "=@" + p));

            string Sql = $@"update {table} set {string.Join(",", pps.Select(p => p + "=@" + p))} where {where}";
            return new SqlCommand(Sql, tableParam, whereParam);
        }

        public static SqlCommand Update<T>(T tableParam, object whereParam, string[] Exclude = null, string[] Include = null)
        {
            return Update<T>(tableParam, null, whereParam, Exclude, Include);
        }
        public static SqlCommand Update<T>(T tableParam, string where, object whereParam, string[] Exclude = null, string[] Include = null)
        {
            var pps = HelperCommon.GetProperty(typeof(T), Exclude, Include).Select(p => p.Name);
            var wps = HelperCommon.GetProperty(whereParam.GetType()).Select(p => p.Name);

            if (string.IsNullOrEmpty(where)) where = string.Join(",", wps.Select(p => p + "=@" + p));

            string Sql = $@"update {typeof(T).Name} set {string.Join(",", pps.Select(p => p + "=@" + p))} where {where}";
            return new SqlCommand(Sql, tableParam, whereParam);
        }

        public static SqlCommand Update<T, TAtt>(T tableParam, object whereParam, string[] Exclude = null, string[] Include = null) where TAtt : Attribute
        {
            return Update<T, TAtt>(tableParam, null, whereParam, Exclude, Include);
        }
        public static SqlCommand Update<T, TAtt>(T tableParam, string where, object whereParam, string[] Exclude = null, string[] Include = null) where TAtt : Attribute
        {
            Dictionary<PropertyInfo, TAtt> dic = HelperCommon.GetPropertyAttribute<TAtt>(typeof(T), Exclude, Include);
            var wps = HelperCommon.GetProperty(whereParam.GetType()).Select(p => p.Name);

            if (string.IsNullOrWhiteSpace(where)) where = string.Join(",", wps.Select(p => p + "=@" + p));
            if (dic.Count == 0)
            {
                return null;
            }
            List<string> ps;
            if (typeof(TAtt) == typeof(SDAttribute))
            {
                ps = dic.Where(p => ((p.Value as SDAttribute).Action & SDAction.Update) == SDAction.Update).Select(p => p.Key.Name).ToList();
            }
            else
            {
                ps = dic.Keys.Select(p => p.Name).ToList();
            }

            string Sql = $@"update {typeof(T).Name} set {string.Join(",", ps.Select(p => p + "=@" + p))} where {where}";
            return new SqlCommand(Sql, tableParam, whereParam);
        }

        #endregion
        #region insert or update
        /// <summary>
        /// 插入或更新
        /// </summary>
        /// <param name="table"></param>
        /// <param name="tableParam"></param>
        /// <param name="where">a.id=b.id</param>
        /// <param name="action"></param>
        /// <returns></returns>
        public static SqlCommand InsertOrUpdate(string table, object tableParam, string where, SDAction action = SDAction.Insert | SDAction.Update)
        {
            var ps = HelperCommon.GetProperty(tableParam.GetType()).Select(p => p.Name);

            string s1 = string.Join(",", ps);
            string s2 = string.Join(",", ps.Select(p => "@" + p));
            string s3 = string.Join(",", ps.Select(p => p + "=@" + p));

            string Sql;
            Sql = string.Format("MERGE INTO {0} A USING (SELECT {1}) B ON {2}"
                , table
                , s3
                , where);
            if ((action & SDAction.Insert) == SDAction.Insert)
            {
                //插入
                Sql += string.Format(" WHEN NOT MATCHED THEN INSERT ({0})VALUES({1})"
                    , s1
                    , s2);
            }

            if ((action & SDAction.Update) == SDAction.Update)
            {
                //更新
                Sql += string.Format(" WHEN MATCHED THEN UPDATE SET {0}"
                    , s3);
            }
            //结束标记
            Sql += ";";
            return new SqlCommand(Sql, tableParam);
        }
        /// <summary>
        /// 插入或更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableParam"></param>
        /// <param name="where"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        public static SqlCommand InsertOrUpdate<T>(T tableParam, string where, SDAction action = SDAction.Insert | SDAction.Update)
        {
            var ps = HelperCommon.GetProperty(typeof(T)).Select(p => p.Name);

            string s1 = string.Join(",", ps);
            string s2 = string.Join(",", ps.Select(p => "@" + p));
            string s3 = string.Join(",", ps.Select(p => p + "=@" + p));

            string Sql;
            Sql = string.Format("MERGE INTO {0} A USING (SELECT {1}) B ON {2}"
                , typeof(T).Name
                , s3
                , where);
            if ((action & SDAction.Insert) == SDAction.Insert)
            {
                //插入
                Sql += string.Format(" WHEN NOT MATCHED THEN INSERT ({0})VALUES({1})"
                    , s1
                    , s2);
            }

            if ((action & SDAction.Update) == SDAction.Update)
            {
                //更新
                Sql += string.Format(" WHEN MATCHED THEN UPDATE SET {0}"
                    , s3);
            }
            //结束标记
            Sql += ";";
            return new SqlCommand(Sql, tableParam);
        }

        /// <summary>
        /// 插入或更新   
        /// </summary>
        /// <param name="table"></param>
        /// <param name="insertParam">insertParam为null不插入</param>
        /// <param name="updateParam">updateParam为null不更新</param>
        /// <param name="whereParam"></param>
        /// <returns></returns>
        public static SqlCommand InsertOrUpdate(string table, object insertParam, object updateParam, object whereParam)
        {
            var wps = HelperCommon.GetProperty(whereParam.GetType()).Select(p => p.Name);

            string Sql;
            Sql = string.Format("MERGE INTO {0} A USING (SELECT {1}) B ON {2}"
                , table
                , string.Join(",", wps.Select(p => p + "=@2_" + p))
                , string.Join(" and ", wps.Select(p => "A." + p + "=B." + p)));
            if (insertParam != null)
            {
                var ips = HelperCommon.GetProperty(insertParam.GetType()).Select(p => p.Name);
                //插入
                Sql += string.Format(" WHEN NOT MATCHED THEN INSERT ({0})VALUES({1})"
                    , string.Join(",", ips.Select(p => p))
                    , string.Join(",", ips.Select(p => "@0_" + p)));
            }

            if (updateParam != null)
            {
                var ups = HelperCommon.GetProperty(updateParam.GetType()).Select(p => p.Name);
                //更新
                Sql += string.Format(" WHEN MATCHED THEN UPDATE SET {0}"
                    , string.Join(",", ups.Select(p => p + "=@1_" + p)));
            }
            //结束标记
            Sql += ";";
            return new SqlCommand(Sql, insertParam, updateParam, whereParam);
        }
        #endregion

    }
}
