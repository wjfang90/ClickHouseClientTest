using ClickHouseClientNet.ClickHouse.Client.Dapper.Test;
using ClickHouseClientNet.ClickHouse.Client.Dapper.Test.Models;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace DapperUnitTest {
    [TestClass]
    public class ClickHouseClientDapperUnitTest {
        [TestMethod]
        public void GetDayCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";

            var sql = @"select count(1) from DbInterface.CustomerAccessLog 
                      where RequestUserID=@customerId and OperationTime>=@startTime and OperationTime<@endTime ";

            var conn = ClickHouseClientDapper.GetConnection();
            var count = conn.QueryFirstOrDefault<long>(sql, new { customerId, startTime, endTime });

            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public void GetHourCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = "2018-07-18 08:52:00";
            var endTime = "2018-07-19 08:53:00";

            var sql = @"select count(1) from DbInterface.CustomerAccessLog 
                      where RequestUserID=@customerId and OperationTime>=@startTime and OperationTime<@endTime ";

            var conn = ClickHouseClientDapper.GetConnection();
            var count = conn.QueryFirstOrDefault<long>(sql, new { customerId, startTime, endTime });
            Assert.AreNotEqual(0, count);
            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public void GetGroupGidCount() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";

            var sql = @"select count(1) as listCount from (
										select distinct Library,Gid from DbInterface.CustomerAccessLog l 
										where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord'  or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
									) t";

            var conn = ClickHouseClientDapper.GetConnection();
            var count = conn.QueryFirstOrDefault<long>(sql, new { customerId, library, startTime, endTime });
            Assert.AreNotEqual(0, count);
            Assert.AreEqual(8, count);
        }

        [TestMethod]
        public void GetGroupGidList() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";
            var pageIndex = 1;
            var pageSize = 5;

            var sql = @"select * from (
                        select distinct Library,Gid from DbInterface.CustomerAccessLog l
						where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord' or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
                        ) t 
                        limit @pageSize offset (@pageIndex-1)*@pageSize";


            var conn = ClickHouseClientDapper.GetConnection();
            var result = conn.Query<SingleRecordLogResult>(sql, new { customerId, library, startTime, endTime, pageIndex, pageSize });

            Assert.IsNotNull(result);
            Assert.AreEqual(pageSize, result.ToList().Count);
        }

        [TestMethod]
        public void GetGroupGidList1() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";
            var pageIndex = 2;
            var pageSize = 5;

            var sql = @"select * from (
                        select distinct Library,Gid from DbInterface.CustomerAccessLog l
						where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord' or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
                        ) t 
                        limit @pageSize offset (@pageIndex-1)*@pageSize";


            var conn = ClickHouseClientDapper.GetConnection();
            var result = conn.Query<SingleRecordLogResult>(sql, new { customerId, library, startTime, endTime, pageIndex, pageSize });

            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.ToList().Count);
        }

        [TestMethod]
        public void QueryList() {
            var top = 3;
            var sql = "select * from CustomerAccessLog limit @top";

            var conn = ClickHouseClientDapper.GetConnection();
            var data = conn.Query(sql, new { top });

            Assert.IsNotNull(data);
            Assert.AreEqual(top, data.ToList().Count);
        }

        [TestMethod]
        public void QueryListByExpandoObject() {
            string customerId = "DZ0Z86HBJ0PB8N2V";
            string library = null;
            var top = 5;
            var sql = "select * from CustomerAccessLog {0} limit @top";

            var conn = ClickHouseClientDapper.GetConnection();

            //Tuple<字段名称,操作符，参数名称>
            List<Tuple<string,string, string>> parameterNameList = new List<Tuple<string,string, string>>();
            ExpandoObject obj = new ExpandoObject();
            var parameterValueList = obj as IDictionary<string, object>;

            if (!string.IsNullOrWhiteSpace(customerId)) {
                parameterNameList.Add(Tuple.Create("RequestUserID", "=","customerId"));
                parameterValueList.Add("customerId", customerId);
            }

            if (!string.IsNullOrWhiteSpace(library)) {
                parameterNameList.Add(Tuple.Create("Library","=", "library"));
                parameterValueList.Add("library", library);
            }

            if (top != default) {
                parameterValueList.Add("top", top);
            }

            var where = string.Empty;

            if (parameterNameList.Any()) {
                where = "where" + string.Join(" and ", parameterNameList.Select(t => $" {t.Item1}{t.Item2}@{t.Item3}"));
            }

            sql = string.Format(sql, where);

            var data = conn.Query<CustomerAccessLog>(sql, obj);//error

            Assert.IsNotNull(data);
            Assert.AreEqual(top, data.ToList().Count);
        }

        [TestMethod]
        public void QueryListError1() {
            string customerId = "DZ0Z86HBJ0PB8N2V";
            string library = null;
            var top = 5;
            var sql = "select * from CustomerAccessLog where RequestUserID=@customerId and Library=@library limit @top";

            var conn = ClickHouseClientDapper.GetConnection();
            var data = conn.Query<CustomerAccessLog>(sql, new { customerId, library, top });//error

            Assert.IsNotNull(data);
            Assert.AreEqual(top, data.ToList().Count);
        }

        [TestMethod]
        public void QueryListError2() {
            string customerId = "DZ0Z86HBJ0PB8N2V";
            string library = null;
            var top = 5;
            var sql = "select * from CustomerAccessLog where RequestUserID=@customerId and Library=@library limit @top";

            var conn = ClickHouseClientDapper.GetConnection();
            var data = conn.Query<CustomerAccessLog>(sql, new { customerId, top });//error

            Assert.IsNotNull(data);
            Assert.AreEqual(top, data.ToList().Count);
        }
       

        [TestMethod]
        public void QueryOne() {
            var id = "1";
            var sql = "select * from CustomerAccessLog where ID=@id limit 1";

            var conn = ClickHouseClientDapper.GetConnection();
            var data = conn.QueryFirstOrDefault<CustomerAccessLog>(sql, new { id });

            Assert.IsNotNull(data);
            Assert.AreEqual(id, data.ID);
        }

        [TestMethod]
        public void AddAccessLog() {
            var model = new CustomerAccessLog() {
                ID = "1",
                MethodName = "test methodname",
                ParameterList = "test parameters",
                Library = "test library",
                Gid = "test gid",
                RequestIP = "192.168.4.179",
                RequestUserID = "test customer id",
                OperationTime = DateTime.Now
            };

            var result = ClickHouseClientDapper.Add(model);

            Assert.AreEqual(0, result);
        }

        [TestMethod]
        public void DeleteAccessLog() {
            var id = "1";
            var sql = "delete from CustomerAccessLog where ID=@id";

            var conn = ClickHouseClientDapper.GetConnection();
            var result = conn.Execute(sql, new { id });
            Assert.AreEqual(0, result);

        }
    }
}
