using Kingdee.BOS;
using Kingdee.BOS.App.Data;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Service
{
    internal class Data
    {

        /// <summary>
        /// 同步数据库配置
        /// </summary>
        #region
        private const string server = "";
        private const string port = "3306";
        private const string user = "";
        private const string password = "";
        private const string database = "";
        readonly MySqlConnection conn = new MySqlConnection($"server={server};port={port};user={user};password={password}; database={database};");
        #endregion
        //金蝶数据库恢复版本
        private const string SQLSERVER = "";

        //最大分页天数
        private const int SYNDAYS = 3;

        //单次执行语句数
        private const int NUMS = 500;

        //操作对象
        private Context context;
        public Data(Context context)
        {
            this.context = context;
        }
        public Context Context { get => context; set => context = value; }

        //本次更新影响数据条数
        private long UPDATENUM = 0;

        //日志
        private string LOG;

        //数据连接状态
        private int state = 0;
        public void SJK()
        {
            try
            {
                if (state == 0)
                {
                    conn.Open();
                    state = 1;
                }
            }
            catch
            {
                //邮件通知
                SendMail sendMail = new SendMail(Context);
                sendMail.Send($"金蝶数据同步无法连接同步数据库{DateTime.Now}", $"无法连接至远程数据库，请及时检查。");
                conn.Close();
                state = 0;
            }
        }

        /// <summary>
        /// 同步错误
        /// </summary>
        /// <param name="CurrentTableName">出错表名</param>
        public void SynError(string ErrorTableName)
        {
            try
            {
                SJK();
                //更改备注
                MySqlCommand com = new MySqlCommand($@"update setting set STATE = '0',METHOD = 'ServicePlug',NOTE = '该表于 {DateTime.Now} 同步出错，请及时检查问题。' where TABLENAME = '{ErrorTableName}';
                                                insert into log(STAUTS,NOTE,METHOD,TIME) values('0','{LOG}{ErrorTableName}表同步出错。','ServicePlug','{DateTime.Now}');", conn);
                com.ExecuteNonQuery();
            }
            catch
            {
                //邮件通知
                SendMail sendMail = new SendMail(Context);
                sendMail.Send($"金蝶数据同步出错{DateTime.Now}", $"数据库可能出现问题，请尽快检查。出错配置表名：{ErrorTableName}");
                state = 0;
                conn.Close();
            }
        }

        /// <summary>
        /// 批量异步执行
        /// </summary>
        /// <param name="sql">语句</param>
        public void Execute(string sql)
        {
            if (sql != "")
            {
                MySqlCommand com = new MySqlCommand(sql, conn);
                com.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// 测试类
        /// </summary>
        public void Test()
        {
            SJK();
            //数据库连接不成功则终止运行
            if (state == 0)
            {
                return;
            }
            string sql = "";
            string CurrentTableName = "";
            try
            {
                MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter("select * from setting where STATE = 1", conn);
                DataTable dt = new DataTable();
                mySqlDataAdapter.Fill(dt);
                mySqlDataAdapter.Dispose();
                //同步配置表同步项为0则不同步
                if (dt.Rows.Count != 0)
                {
                    //分行执行
                    foreach (DataRow dr in dt.Rows)
                    {
                        UPDATENUM = 0;
                        //同步前数据校验
                        #region
                        CurrentTableName = string.IsNullOrWhiteSpace(Convert.ToString(dr["TABLENAME"]))
                    ? "" : Convert.ToString(dr["TABLENAME"]);
                        string LASTTIME = string.IsNullOrWhiteSpace(Convert.ToString(dr["LASTTIME"]))
                    ? SQLSERVER : Convert.ToString(dr["LASTTIME"]);
                        //避免为负
                        long FREQUENCY = Convert.ToInt64(dr["FREQUENCY"]) >= 0 ? Convert.ToInt64(dr["FREQUENCY"]) : 5;
                        if (CurrentTableName == "")
                        {
                            //待同步表名为空退出
                            continue;
                        }
                        #endregion

                        //清理冗余
                        if (Convert.ToInt64(dr["DELETESTATE"]) == 1 && !CurrentTableName.ToLower().Contains("group"))
                        {
                            //频率
                            long DELETEFREQUENCY = Convert.ToInt64(dr["DELETEFREQUENCY"]) >= 0 ? Convert.ToInt64(dr["DELETEFREQUENCY"]) : 60;
                            //上次清理时间
                            string DELETELASTTIME = string.IsNullOrWhiteSpace(Convert.ToString(dr["DELETELASTTIME"]))
                        ? SQLSERVER : Convert.ToString(dr["DELETELASTTIME"]);
                            //清理表主键
                            string DELETEPRIKEY = string.IsNullOrWhiteSpace(Convert.ToString(dr["DELETEPRIKEY"]))
                        ? "" : Convert.ToString(dr["DELETEPRIKEY"]);

                            sql += DeleteData(CurrentTableName, DELETEPRIKEY, DELETEFREQUENCY, DELETELASTTIME, Convert.ToString(dr["SQL"]));
                        }

                        //拼接所有同步表sql
                        sql += SynData(CurrentTableName, LASTTIME, FREQUENCY, Convert.ToString(dr["SQL"]));
                        if (UPDATENUM > NUMS)
                        {
                            Execute(sql);
                        }
                    }
                    //插入日志
                    Execute($"{sql} insert into log(STAUTS,NOTE,METHOD,TIME) values('1','{LOG}','ServicePlug','{DateTime.Now}');");
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex);
                SynError(CurrentTableName);
            }
        }
        /// <summary>
        /// 删除冗余数据
        /// </summary>
        /// <param name="TABLE">表名</param>
        /// <param name="sqltxt">SQL语句</param>
        /// <returns></returns>
        public string DeleteData(string TABLE, string DELETEPRIKEY, long DELETEFREQUENCY, string DELETELASTTIME, string sqltxt)
        {
            var time = DateTime.Now;
            try
            {
                //上次更新/修改日期
                var date1 = Convert.ToDateTime(DELETELASTTIME);
                //更新频率 若还未到时间则暂时跳过
                if (date1.AddMinutes(DELETEFREQUENCY - 1) >= time)
                {
                    return "";
                }
                string sql = "";
                int num = 0;

                string keytxt = DELETEPRIKEY.Split(new string[] { "." }, StringSplitOptions.RemoveEmptyEntries)[1];
                //源表
                DataTable Sourcetable = DBUtils.ExecuteDataSet(Context, $"/*dialect*/select {DELETEPRIKEY} from " + sqltxt.Split(new string[] { "from", "FROM", "where", "WHERE" }, StringSplitOptions.RemoveEmptyEntries)[1]).Tables[0];
                //同步表
                MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter($"select {keytxt} from {TABLE}", conn);
                DataTable Syntable = new DataTable();
                mySqlDataAdapter.Fill(Syntable);
                mySqlDataAdapter.Dispose();
                IEnumerable<DataRow> itemDataTable = Syntable.AsEnumerable().Except(Sourcetable.AsEnumerable(), DataRowComparer.Default);
                //遍历
                foreach (var item in itemDataTable)
                {
                    sql += $@"delete from {TABLE} where {keytxt} = '{item[keytxt]}';";
                    num++;
                    if (num % NUMS == 0)
                    {
                        Execute(sql);
                        sql = "";
                    }
                }
                LOG += $"{TABLE}表清理冗余{num}条；";
                return sql += $@"update setting set METHOD = 'ServicePlug',DELETELASTTIME = '{time}',NOTE = '本次清理了 {num} 条数据。' where TABLENAME = '{TABLE}';"; ;
            }
            catch
            {
                LOG += $"{TABLE}表冗余清理失败；";
                return $"update setting set DELETESTATE = '0',METHOD = 'ServicePlug',DELETELASTTIME = '{time}',NOTE = '冗余清理出错。' where TABLENAME = '{TABLE}';";
            }
        }
        /// <summary>
        /// 同步数据
        /// </summary>
        /// <param name="TABLE">表名</param>
        /// <param name="LASTTIME">上次同步时间</param>
        public string SynData(string TABLE, string LASTTIME, long FREQUENCY, string sqltxt)
        {
            var time = DateTime.Now;
            //上次更新/修改日期
            var date1 = Convert.ToDateTime(LASTTIME);
            //更新频率 若还未到时间则暂时跳过
            if (date1.AddMinutes(FREQUENCY - 1) >= time)
            {
                return "";
            }
            //获取同步表字段读入列表
            List<string> FiledName = new List<string>();
            string sql = "";
            MySqlCommand com = new MySqlCommand($"show columns from {TABLE}", conn);
            MySqlDataReader dataReader = com.ExecuteReader();
            if (dataReader.HasRows && sqltxt != "")
            {
                while (dataReader.Read())
                {
                    FiledName.Add(dataReader.GetString("Field"));
                }
            }
            else
            {
                dataReader.Close();
                //无数据则返回空值
                LOG += $"{TABLE}表存在配置错误，同步失败；";
                return sql += $@"update setting set STATE = '0',METHOD = 'ServicePlug',LASTTIME = '{time}',NOTE = '该表字段为空或同步所需SQL为空，请检查后再试。' where TABLENAME = '{TABLE}';";
            }
            dataReader.Close();

            //当时间间隔大于最大同步分页天数则进行分组更新(分组表由于不存在时间间隔故默认一次完成)
            if (time.Subtract(date1).TotalDays > SYNDAYS && !TABLE.ToLower().Contains("group"))
            {
                while (time.Subtract(date1).TotalDays >= 0)
                {
                    //增加日期
                    var date2 = date1.AddDays(SYNDAYS);
                    //分日期更新
                    Execute(JointSQL(FiledName, TABLE, sqltxt, date1, date2));
                    date1 = date2;
                }
            }
            else
            {
                sql = JointSQL(FiledName, TABLE, sqltxt, date1, time);
            }
            LOG += $"{TABLE}更新{UPDATENUM}条；";
            return sql += $@"update setting set METHOD = 'ServicePlug',LASTTIME = '{time}',NOTE = '本次更新了 {UPDATENUM} 条数据。' where TABLENAME = '{TABLE}';";
        }
        public string JointSQL(List<string> FiledName, string TABLE, string sqltxt, DateTime date1, DateTime date2)
        {
            string sql = "";
            IEnumerable<object> itemDataTable;
            //分组列表由于不存在创建修改时间 故更新时只能通过遍历差集进行更新
            if (TABLE.ToLower().Contains("group"))
            {
                //源表
                DataTable Sourcetable = DBUtils.ExecuteDataSet(Context, string.Format("/*dialect*/" + sqltxt)).Tables[0];
                //同步表
                MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter($"select * from {TABLE}", conn);
                DataTable Syntable = new DataTable();
                mySqlDataAdapter.Fill(Syntable);
                mySqlDataAdapter.Dispose();
                //去除多余的修改时间列
                Syntable.Columns.Remove("FMODIFYDATE");
                itemDataTable = Sourcetable.AsEnumerable().Except(Syntable.AsEnumerable(), DataRowComparer.Default);
            }
            else
            {
                //读取金蝶表待同步内容
                itemDataTable = DBUtils.ExecuteEnumerable(Context, string.Format("/*dialect*/" + sqltxt, date1, date2));
            }
            //遍历
            foreach (var item in itemDataTable)
            {
                //拼接字符串
                string filed = "", value = "", update = "";
                foreach (string name in FiledName)
                {
                    string VALUE;
                    if (TABLE.ToLower().Contains("group"))
                    {
                        if (name == "FMODIFYDATE")
                        {
                            //同步时间修改为当前时间
                            VALUE = date2.ToString();
                        }
                        else
                        {
                            VALUE = string.IsNullOrWhiteSpace(Convert.ToString(((DataRow)item)[name]))
                            ? "" : Convert.ToString(((DataRow)item)[name]);
                        }
                    }
                    else
                    {
                        VALUE = string.IsNullOrWhiteSpace(Convert.ToString(((IDataRecord)item)[name]))
                            ? "" : Convert.ToString(((IDataRecord)item)[name]);
                    }
                    filed += $"{name},";//字段
                    value += $"'{VALUE}',";//值
                    update += $"{name} = '{VALUE}',";//修改格式
                }
                //去除句尾逗号
                sql += $@"insert into {TABLE}({filed.Substring(0, filed.Length - 1)})
                            values ({value.Substring(0, value.Length - 1)}) 
                            ON DUPLICATE KEY UPDATE {update.Substring(0, update.Length - 1)}
                        ;";
                UPDATENUM++;
                if (UPDATENUM % NUMS == 0)
                {
                    Execute(sql);
                    sql = "";
                }
            }
            return sql;
        }
    }
}