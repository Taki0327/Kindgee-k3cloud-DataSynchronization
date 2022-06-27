using Kingdee.BOS;
using Kingdee.BOS.App.Core;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Service
{
    internal class SendMail
    {
        //操作对象
        private Context context;
        public SendMail(Context context)
        {
            this.context = context;
        }
        public Context Context { get => context; set => context = value; }

        /// <summary>
        /// 收件人列表
        /// </summary>
        /// <returns></returns>
        public List<string> SelectMail()
        {
            List<string> getuser = new List<string>();
            IDataReader reader = DBUtils.ExecuteReader(Context, "");
            while (reader.Read())
            {
                getuser.Add(reader[0].ToString());
            }
            reader.Close();
            if (getuser.Count > 0)
            {
                return getuser;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 发送邮件
        /// </summary>
        /// <param name="title">标题</param>
        /// <param name="txt">正文</param>
        public void Send(string title,string txt)
        {
            //发送文件
            #region
            //File file = ;
            //StreamReader streamReader = new StreamReader(file.Filepath);

            //FileStream output = new FileStream(file.Filepath, FileMode.Open);
            //Dictionary<string, Stream> dic = new Dictionary<string, Stream>
            //{
            //    //{ file.Filename, streamReader.BaseStream }
            //    {file.Filename,output}
            //};
            #endregion
            //读取预设收件人
            var getuser = SelectMail();
            if (getuser != null)
            {
                try
                {
                    foreach (var item in getuser)
                    {
                        try
                        {
                            //调用金蝶预设虚拟邮件账户，该处请根据开发文档(https://github.com/Taki0327/Secondary-Development-of-Kindgee-k3cloud)中步骤提前配置。
                            var email = new SendMailService().GetEmailMessageInfoByBosVirtual(Context, 0);
                            if (email != null)
                            {
                                email.To = new List<string> { item };
                                email.Subject = title;
                                email.Body = txt;
                                MailUtils.Sendmail(email);
                            }
                        }
                        catch
                        {

                        }
                    }
                }
                catch
                {

                }
                finally
                {
                    //output.Close();
                }
            }
        }
    }
}
