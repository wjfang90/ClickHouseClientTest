using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseClientNetCoreSqlSugar.Test.Models {

    public class CustomerAccessLog {

        public string ID { get; set; }
        public string MethodName { get; set; }
        public string ParameterList { get; set; }
        public string Library { get; set; }
        public string Gid { get; set; }
        public string RequestIP { get; set; }
        public string RequestUserID { get; set; }
        public string RequestUserName { get; set; }
        public DateTime OperationTime { get; set; }

    }
}
