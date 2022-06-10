using Kingdee.BOS;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Service
{
    internal class Service : IScheduleService
    {
        public void Run(Context ctx, Schedule schedule)
        {
            Data data = new Data(ctx);
            data.Test();
        }
    }
}
