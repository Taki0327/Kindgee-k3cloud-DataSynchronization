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
        //最大分页天数
        private const int SYNDAYS = 3;
        //操作对象
        private Context context;

        public Data(Context context)
        {
            this.context = context;
        }

        public Context Context { get => context; set => context = value; }

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
                MySqlCommand com = new MySqlCommand($@"update setting set STATE = '0',METHOD = 'ServicePlug',NOTE = '该表于 {DateTime.Now} 同步出错，请及时检查问题。' where TABLENAME = '{ErrorTableName}'", conn);
                com.ExecuteNonQueryAsync();
            }
            catch
            {
                state = 0;
                conn.Close();
            }
        }
        
        /// <summary>
        /// SQL语句对应
        /// </summary>
        /// <param name="TABLE">表名</param>
        /// <returns>语句文本</returns>
        public string SQLTXT(string TABLE)
        {
            string sqltxt = "";
            switch(TABLE)
            {
                
            }
            return sqltxt;
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
            if(state == 0)
            {
                return;
            }
            string sql = "";
            string CurrentTableName="";
            try
            {
                MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter("select * from setting where STATE = 1", conn);
                DataTable dt = new DataTable();
                mySqlDataAdapter.Fill(dt);
                mySqlDataAdapter.Dispose();
                //同步配置表同步项为0则不同步
                if (dt.Rows.Count != 0)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        //同步前数据校验
                        #region
                        CurrentTableName = string.IsNullOrWhiteSpace(Convert.ToString(dr["TABLENAME"]))
                   ? "" : Convert.ToString(dr["TABLENAME"]);
                        string LASTTIME = string.IsNullOrWhiteSpace(Convert.ToString(dr["LASTTIME"]))
                   ? "" : Convert.ToString(dr["LASTTIME"]);
                        if (CurrentTableName == "")
                        {
                            //待同步表名为空退出
                            continue;
                        }
                        else if(LASTTIME == "")
                        {
                            MySqlCommand com = new MySqlCommand($"select FMODIFYDATE from {CurrentTableName} order by FMODIFYDATE desc limit 1", conn);
                            LASTTIME = Convert.ToString(com.ExecuteScalar());
                            if (string.IsNullOrWhiteSpace(LASTTIME))
                            {
                                //待同步表中无数据则退出
                                continue;
                            }
                        }
                        #endregion
                        //拼接所有同步表sql
                        //sql += SynData(CurrentTableName, LASTTIME, SQLTXT(CurrentTableName));
                        sql += SynData(CurrentTableName, LASTTIME, Convert.ToString(dr["SQL"]));
                    }
                    Execute(sql);
                }
            }
            catch
            {
                SynError(CurrentTableName);
            }
        }
        /// <summary>
        /// 同步数据
        /// </summary>
        /// <param name="TABLE">表名</param>
        /// <param name="LASTTIME">上次同步时间</param>
        public string SynData(string TABLE, string LASTTIME , string sqltxt)
        {
            var time = DateTime.Now;
            //获取同步表字段读入列表
            List<string> FiledName = new List<string>();
            string sql = "";
            MySqlCommand com = new MySqlCommand($"show columns from {TABLE}", conn);
            MySqlDataReader dataReader = com.ExecuteReader();
            if(dataReader.HasRows && sqltxt != "")
            {
                while(dataReader.Read())
                {
                    FiledName.Add(dataReader.GetString("Field"));
                }
            }
            else
            { 
                //无数据则返回空值
                return "";
            }
            dataReader.Close();
            //读取金蝶表待同步内容
            //上次更新/修改日期
            var date1 = Convert.ToDateTime(LASTTIME);
            //当时间间隔大于最大同步分页天数则进行分组更新
            if (time.Subtract(date1).TotalDays > SYNDAYS)
            {
                while(time.Subtract(date1).TotalDays >= 0)
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
                sql = JointSQL(FiledName, TABLE, sqltxt, date1,time);
            }
            return sql +=$@"update setting set METHOD = 'ServicePlug',LASTTIME = '{time}' where TABLENAME = '{TABLE}';" ;
        }
        public string JointSQL(List<string> FiledName,string TABLE,string sqltxt, DateTime date1,DateTime date2)
        {
            string sql = ""; 
            IEnumerable<IDataRecord> itemDataTable = DBUtils.ExecuteEnumerable(Context, string.Format("/*dialect*/" + sqltxt, date1, date2));
            foreach (IDataRecord item in itemDataTable)
            {
                //拼接字符串
                string filed = "", value = "", update = "";
                foreach (string name in FiledName)
                {
                    string VALUE = string.IsNullOrWhiteSpace(Convert.ToString(item[name]))
                    ? "" : Convert.ToString(item[name]);
                    filed += $"{name},";//字段
                    value += $"'{VALUE}',";//值
                    update += $"{name} = '{VALUE}',";//修改格式
                }
                //去除句尾逗号
                sql += $@"insert into {TABLE}({filed.Substring(0, filed.Length - 1)})
                            values ({value.Substring(0, value.Length - 1)}) 
                            ON DUPLICATE KEY UPDATE {update.Substring(0, update.Length - 1)}
                        ;";
            }
            return sql;
        }
    }
}
