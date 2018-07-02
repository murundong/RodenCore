using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Sql
{

    public class SqlHelper
    {
        public const string specialChar = "__";



        /// <summary>
        /// 异步回调函数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="err"></param>
        public delegate void OnCallBack<T>(T data, string err);
        /// <summary>
        /// 查询委托
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        protected delegate T OnSelectSql<T>(string sql, out string err);
        /// <summary>
        /// 查询委托
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlDic"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        protected delegate T OnSelectDic<T>(Dictionary<string, string> sqlDic, out string err);
        /// <summary>
        /// 执行委托
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        protected delegate int OnRunSql<T>(T sql, out string err);


        /// <summary>
        /// 单例
        /// </summary>
        public static SqlHelper Instance = new SqlHelper();

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public string ConnStr { get; set; }

        /// <summary>
        /// sql指令执行超时时间，默认60s
        /// </summary>
        public int CommandTimeout = 60;

     
    

        /// <summary>
        /// 数据库连接类型
        /// </summary>
        public ConnectionType ConnType { get; set; }

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <returns></returns>
        private IDbConnection CreateConnection()
        {
            switch (ConnType)
            {
                case ConnectionType.Access:
                    return new System.Data.OleDb.OleDbConnection(ConnStr);
                case ConnectionType.MsSql:
                    return new System.Data.SqlClient.SqlConnection(ConnStr);
                case ConnectionType.MySql:
                    return new MySql.Data.MySqlClient.MySqlConnection(ConnStr);
            }
            throw new Exception("还未实现的数据库类型");
        }

        /// <summary>
        /// 创建sqladapter
        /// </summary>
        /// <returns></returns>
        private IDbDataAdapter CreateDataAdapter()
        {
            switch (ConnType)
            {
                case ConnectionType.Access:
                    return new System.Data.OleDb.OleDbDataAdapter();
                case ConnectionType.MsSql:
                    return new System.Data.SqlClient.SqlDataAdapter();
                case ConnectionType.MySql:
                    return new MySql.Data.MySqlClient.MySqlDataAdapter();
            }
            throw new Exception("还未实现的数据库类型");
        }


        #region Run_Sql_Return_Int
        /// <summary>
        /// 异步返回受影响的行数
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        public Task<int> AsyncRun_Sql_Return_Int(string cmdText, OnCallBack<int> onCallBack)
        {
            return AsyncRun_Sql_Return_Int(new string[] { cmdText }, onCallBack);
        }
        public Task<int> AsyncRun_Sql_Return_Int(string cmdText, object param, OnCallBack<int> onCallBack)
        {
            return AsyncRun_Sql_Return_Int(new SqlCommand[] { new SqlCommand(cmdText, param) }, onCallBack);
        }
        public Task<int> AsyncRun_Sql_Return_Int(SqlCommand cmd, OnCallBack<int> onCallBack)
        {
            return AsyncRun_Sql_Return_Int(new SqlCommand[] { cmd }, onCallBack);
        }
        public Task<int> AsyncRun_Sql_Return_Int(IEnumerable<string> sqlList, OnCallBack<int> onCallBack)
        {
            return AsyncRun_Sql_Return_Int(sqlList.Select(p => new SqlCommand(p, null)), onCallBack);
        }
        public Task<int> AsyncRun_Sql_Return_Int(IEnumerable<SqlCommand> cmdList, OnCallBack<int> onCallBack)
        {
            string err = null;
            Task<int> task = Task.Factory.StartNew<int>(() =>
            {
                return Run_Sql_Return_Int(cmdList, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }

        /// <summary>
        /// 返回受影响的行数
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public int Run_Sql_Return_Int(string cmdText, out string err)
        {
            return Run_Sql_Return_Int(cmdText, true, out err);
        }
        public int Run_Sql_Return_Int(string cmdText, bool openTran, out string err)
        {
            return Run_Sql_Return_Int(new string[] { cmdText }, openTran, out err);
        }
        public int Run_Sql_Return_Int(string cmdText, object param, out string err)
        {
            return Run_Sql_Return_Int(cmdText, param, true, out err);
        }
        public int Run_Sql_Return_Int(string cmdText, object param, bool openTran, out string err)
        {
            return Run_Sql_Return_Int(new SqlCommand[] { new SqlCommand(cmdText, param) }, openTran, out err);
        }
        public int Run_Sql_Return_Int(SqlCommand cmd, out string err)
        {
            return Run_Sql_Return_Int(cmd, true, out err);
        }
        public int Run_Sql_Return_Int(SqlCommand cmd, bool openTran, out string err)
        {
            return Run_Sql_Return_Int(new SqlCommand[] { cmd }, openTran, out err);
        }
        public int Run_Sql_Return_Int(IEnumerable<string> sqlList, out string err)
        {
            return Run_Sql_Return_Int(sqlList, true, out err);
        }
        public int Run_Sql_Return_Int(IEnumerable<string> sqlList, bool openTran, out string err)
        {
            return Run_Sql_Return_Int(sqlList.Select(p => new SqlCommand(p, null)).ToArray(), openTran, out err);
        }
        public int Run_Sql_Return_Int(IEnumerable<SqlCommand> cmdList, out string err)
        {
            return Run_Sql_Return_Int(cmdList, true, out err);
        }
        public int Run_Sql_Return_Int(IEnumerable<SqlCommand> cmdList, bool openTran, out string err)
        {
            err = null;
            using (var cn = CreateConnection())
            {
                IDbTransaction tran = null;
                int flag = 0;
                try
                {
                    cn.Open();
                    if (openTran) tran = cn.BeginTransaction();

                    foreach (var cmd in cmdList)
                    {
                        var com = cmd.CreateDbCommand(cn);
                        if (tran != null) com.Transaction = tran;
                        com.CommandTimeout = CommandTimeout;

                        int num = com.ExecuteNonQuery();
                        flag += (num > 0 ? num : 0);
                    }
                    if (tran != null) tran.Commit();
                    return flag;
                }
                catch (Exception e)
                {
                    err = e.Message;
                    if (tran != null) tran.Rollback();
                    return int.MinValue;
                }

            }
        }
        #endregion

        #region Select_Sql_Return_Scalar
        /// <summary>
        /// 异步返回查询对象
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        public Task<T> AsyncSelect_Sql_Return_Scalar<T>(string cmdText, OnCallBack<T> onCallBack)
        {
            return AsyncSelect_Sql_Return_Scalar(cmdText, null, onCallBack);
        }
        public Task<T> AsyncSelect_Sql_Return_Scalar<T>(string cmdText, object param, OnCallBack<T> onCallBack)
        {
            return AsyncSelect_Sql_Return_Scalar(new SqlCommand(cmdText, param), onCallBack);
        }
        public Task<T> AsyncSelect_Sql_Return_Scalar<T>(SqlCommand cmd, OnCallBack<T> onCallBack)
        {
            string err = null;
            Task<T> task = Task.Factory.StartNew<T>(() =>
            {
                return Select_Sql_Return_Scalar<T>(cmd, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        /// <summary>
        /// 返回查询对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="err"></param>
        /// <returns></returns> 
        public T Select_Sql_Return_Scalar<T>(string sql, out string err)
        {
            return Select_Sql_Return_Scalar<T>(sql, null, out err);
        }
        public T Select_Sql_Return_Scalar<T>(string sql, object param, out string err)
        {
            return Select_Sql_Return_Scalar<T>(new SqlCommand(sql, param), out err);
        }
        public T Select_Sql_Return_Scalar<T>(SqlCommand cmd, out string err)
        {
            err = null;
            using (var cn = CreateConnection())
            {
                try
                {
                    cn.Open();
                    var com = cmd.CreateDbCommand(cn);
                    com.CommandTimeout = CommandTimeout;

                    object rs = com.ExecuteScalar();
                    com.Parameters.Clear();
                    return (T)Convert.ChangeType(rs, typeof(T));
                }
                catch (Exception e)
                {
                    err = e.Message;
                    return default(T);
                }
            }
        }
        #endregion

        #region Select_Sql_Return_List
        /// <summary>
        /// 异步返回List
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        public Task<List<T>> AsyncSelect_Sql_Return_List<T>(string cmdText, OnCallBack<List<T>> onCallBack)
        {
            return AsyncSelect_Sql_Return_List<T>(cmdText, onCallBack);
        }
        public Task<List<T>> AsyncSelect_Sql_Return_List<T>(string cmdText, object param, OnCallBack<List<T>> onCallBack)
        {
            return AsyncSelect_Sql_Return_List<T>(new SqlCommand(cmdText, param), onCallBack);
        }
        public Task<List<T>> AsyncSelect_Sql_Return_List<T>(SqlCommand cmd, OnCallBack<List<T>> onCallBack)
        {

            string err = null;
            Task<List<T>> task = Task.Factory.StartNew<List<T>>(() =>
            {
                return Select_Sql_Return_List<T>(cmd, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        /// <summary>
        /// 返回List
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="err"></param>
        /// <returns></returns> 
        public List<T> Select_Sql_Return_List<T>(string cmdText, out string err)
        {
            return Select_Sql_Return_List<T>(cmdText, null, out err);
        }
        public List<T> Select_Sql_Return_List<T>(string cmdText, object param, out string err)
        {
            return Select_Sql_Return_List<T>(new SqlCommand(cmdText, param), out err);
        }
        public List<T> Select_Sql_Return_List<T>(SqlCommand cmd, out string err)
        {
            DataTable DT = Select_Sql_Return_DataTable(cmd, out err);
            if (DT == null || DT.Columns.Count == 0)
            {
                return null;
            }
            var query = from r in DT.AsEnumerable()
                        select r.Field<T>(0);
            return query.ToList();
        }
        #endregion

        #region Select_Sql_Return_DataTable
        /// <summary>
        /// 异步返回DataTable
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        public Task<DataTable> AsyncSelect_Sql_Return_DataTable(string cmdText, OnCallBack<DataTable> onCallBack)
        {
            return AsyncSelect_Sql_Return_DataTable(cmdText, null, onCallBack);
        }
        public Task<DataTable> AsyncSelect_Sql_Return_DataTable(string cmdText, object param, OnCallBack<DataTable> onCallBack)
        {
            return AsyncSelect_Sql_Return_DataTable(new SqlCommand(cmdText, param), onCallBack);
        }
        public Task<DataTable> AsyncSelect_Sql_Return_DataTable(SqlCommand cmd, OnCallBack<DataTable> onCallBack)
        {
            string err = null;
            Task<DataTable> task = Task.Factory.StartNew<DataTable>(() =>
            {
                return Select_Sql_Return_DataTable(cmd, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        /// <summary>
        /// 返回DataTable
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public DataTable Select_Sql_Return_DataTable(string sql, out string err)
        {
            return Select_Sql_Return_DataTable(sql, null, out err);
        }
        public DataTable Select_Sql_Return_DataTable(string sql, object param, out string err)
        {
            return Select_Sql_Return_DataTable(new SqlCommand(sql, param), out err);
        }
        public DataTable Select_Sql_Return_DataTable(SqlCommand cmd, out string err)
        {
            var ds = Select_Sql_Return_DataSet(cmd, out err);
            if (ds == null || ds.Tables.Count == 0)
            {
                return null;
            }
            return ds.Tables[0];
        }

        #endregion

        #region Select_Sql_Return_DataSet
        /// <summary>
        /// 异步返回DataSet
        /// </summary>
        /// <param name="cmdDic"></param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        public Task<DataSet> AsyncSelect_Sql_Return_DataSet(Dictionary<string, string> cmdDic, OnCallBack<DataSet> onCallBack)
        {
            string err = null;
            Task<DataSet> task = Task.Factory.StartNew<DataSet>(() =>
            {
                return Select_Sql_Return_DataSet(cmdDic, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        public Task<DataSet> AsyncSelect_Sql_Return_DataSet(SqlCommand cmd, OnCallBack<DataSet> onCallBack)
        {
            string err = null;
            Task<DataSet> task = Task.Factory.StartNew<DataSet>(() =>
            {
                return Select_Sql_Return_DataSet(cmd, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        /// <summary>
        /// 返回DataSet
        /// </summary>
        /// <param name="cmdDic"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public DataSet Select_Sql_Return_DataSet(Dictionary<string, string> cmdDic, out string err)
        {
            var names = cmdDic.Keys.ToArray();
            var values = cmdDic.Values.ToArray();
            SqlCommand cmd = new SqlCommand(string.Join(" \n ", values), null);
            DataSet ds = Select_Sql_Return_DataSet(cmd, out err);
            if (ds == null)
            {
                return null;
            }
            for (int i = 0; i < ds.Tables.Count; i++)
            {
                ds.Tables[i].TableName = names[i];
            }
            return ds;
        }
        public DataSet Select_Sql_Return_DataSet(SqlCommand cmd, out string err)
        {
            err = null;
            using (var cn = CreateConnection())
            {
                try
                {
                    cn.Open();

                    var com = cmd.CreateDbCommand(cn);
                    com.CommandTimeout = CommandTimeout;

                    DataSet ds = new DataSet();
                    var apt = CreateDataAdapter();
                    apt.SelectCommand = com;
                    apt.Fill(ds);
                    return ds;
                }
                catch (Exception e)
                {
                    err = e.Message;
                    return null;
                }
            }
        }
        #endregion

        #region Bulk
        /// <summary>
        /// 数据批量导入
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="batchSize">每一批次中的行数。 在每一批次结束时，将该批次中的行发送到服务器。最小1000</param>
        /// <param name="onCallBack"></param>
        /// <returns></returns>
        [Obsolete("此方法只支持MsSql")]
        public Task<bool> AsyncSqlBulkCopy(DataTable dt, int batchSize, OnCallBack<bool> onCallBack)
        {
            string err = null;
            Task<bool> task = Task.Factory.StartNew<bool>(() =>
            {
                return SqlBulkCopy(dt, batchSize, out err);
            });
            task.ContinueWith((t) =>
            {
                if (onCallBack != null) onCallBack(t.Result, err);
            }, TaskScheduler.FromCurrentSynchronizationContext());
            return task;
        }
        /// <summary>
        /// 数据批量导入
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="BatchSize">每一批次中的行数。 在每一批次结束时，将该批次中的行发送到服务器。最小1000</param>
        /// <returns></returns>
        [Obsolete("此方法只支持MsSql")]
        public bool SqlBulkCopy(DataTable dt, int batchSize, out string err)
        {
            err = null;
            batchSize = batchSize < 1000 ? 1000 : batchSize;
            if (ConnType != ConnectionType.MsSql)
            {
                throw new Exception("MsSqlBulkCopy 仅支持 MsSql");
            }
            try
            {
                using (System.Data.SqlClient.SqlBulkCopy sbc = new System.Data.SqlClient.SqlBulkCopy(this.ConnStr))
                {
                    sbc.BatchSize = batchSize;
                    sbc.BulkCopyTimeout = this.CommandTimeout;
                    sbc.DestinationTableName = dt.TableName;
                    foreach (DataColumn item in dt.Columns)
                    {
                        sbc.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                    }
                    sbc.WriteToServer(dt);
                }
                return true;
            }
            catch (Exception e)
            {
                err = e.Message;
                return false;
            }
        }
        #endregion

        #region other
        /// <summary>
        /// 分页获取数据
        /// </summary>
        /// <param name="table"></param> 
        /// <param name="pageQuery"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public DataTable Select_Sql_Return_Page(string table, SqlPageQuery pageQuery, out string err)
        {
            return Select_Sql_Return_Page(new SqlCommand(table, null), pageQuery, out err);
        }
        public DataTable Select_Sql_Return_Page(SqlCommand cmd, SqlPageQuery pageQuery, out string err)
        {
            if (string.IsNullOrEmpty(pageQuery.Sort))
            {
                throw new Exception("Sort不可为空");
            }
            int count = Select_Sql_Return_Count(cmd.CommandText, cmd.Params);

            pageQuery.TotalCount = count;

            string Sql = @"SELECT TOP {0} * FROM
                    (
                        SELECT ROW_NUMBER() OVER (ORDER BY {1}) AS RowNumber,* FROM ({2}) AS _TABLE_A
                    )AS _TABLE_B
                    WHERE RowNumber>{3} ORDER BY RowNumber";
            Sql = string.Format(Sql
                , pageQuery.PageSize
                , pageQuery.Sort
                , cmd.CommandText
                , pageQuery.PageSize * (pageQuery.PageIndex - 1));
            cmd.CommandText = Sql;
            DataTable DT = Select_Sql_Return_DataTable(cmd, out err);
            return DT;
        }

        private int Select_Sql_Return_Count(string table, object param)
        {
            string err;
            string Sql = "SELECT COUNT(1) FROM (" + table + ") AS _TABLE_A";
            int count = Select_Sql_Return_Scalar<int>(Sql, param, out err);
            return count;
        }
        /// <summary>
        /// 获取最大编号
        /// </summary>
        /// <param name="table"></param>
        /// <param name="col"></param>
        /// <param name="head"></param>
        /// <param name="len"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public string GetMaxID(string table, string col, string head, int len, out string err)
        {
            string Sql = string.Format(@" 
                    SELECT MAX({0}) FROM {1} WHERE {0} LIKE '{2}%' AND LEN({0}) = {3}; 
                        ",
                   col, table, head, head.Length + len);
            string rs = Select_Sql_Return_Scalar<string>(Sql, out err);
            return rs;
        }
        /// <summary>
        /// 创建最大编号
        /// </summary>
        /// <param name="table"></param>
        /// <param name="col"></param>
        /// <param name="head"></param>
        /// <param name="len"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public string NewAutoID(string table, string col, string head, int len, out string err)
        {
            return NewAutoID(table, col, head, len, null, out err);
        }
        /// <summary>
        /// 创建最大编号
        /// </summary>
        /// <param name="table"></param>
        /// <param name="col"></param>
        /// <param name="head"></param>
        /// <param name="len"></param>
        /// <param name="where"></param>
        /// <param name="err"></param>
        /// <returns></returns>
        public string NewAutoID(string table, string col, string head, int len, Dictionary<string, string> where, out string err)
        {
            err = null;
            if (string.IsNullOrEmpty(table) || string.IsNullOrEmpty(col) || string.IsNullOrEmpty(head) || len <= 0)
            {
                err = "参数错误";
                return null;
            }
            if (head.Length + len > 100)
            {
                err = "编码长度不可以超过100位";
                return null;
            }
            bool hasWhere = where != null && where.Count > 0;
            string Sql = string.Format(@"
                    DECLARE @ID NVARCHAR(200),@HEAD NVARCHAR(100),@NO INT,@LEN INT;
                    SET @HEAD = '{2}';
                    SET @LEN = {3};

                    SELECT @ID = {0} FROM {1} WHERE {4}
                    IF @ID IS NOT NULL
                    BEGIN
	                    SELECT @ID
	                    RETURN
                    END                    

                    SELECT @ID = MAX({0}) FROM {1} WHERE {0} LIKE '{2}%' AND LEN({0}) = LEN(@HEAD) + @LEN;
 
	                if @ID IS NULL
		                SET @NO = 1;
	                else
		                SET @NO=CAST(RIGHT(@ID,@LEN) AS INT) + 1;

	                SET @ID = @HEAD + RIGHT('{5}'+CAST(@NO AS VARCHAR),@LEN);
	                INSERT INTO {1} ({6})VALUES({7});
	                SELECT @ID 
                        ",
                    col, table, head, len
                    , hasWhere ? string.Join(" AND ", where.Select(p => p.Key + "=" + p.Value)) : "1 = 2"
                    , string.Empty.PadLeft(len, '0')
                    , hasWhere ? (col + "," + string.Join(",", where.Keys)) : col
                    , hasWhere ? ("@ID," + string.Join(",", where.Values)) : "@ID");

            string rs = Select_Sql_Return_Scalar<string>(Sql, out err);
            return rs;
        }
        #endregion


    }
}
