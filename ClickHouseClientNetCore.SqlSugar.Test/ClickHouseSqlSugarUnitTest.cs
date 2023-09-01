using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouseClientNetCore.SqlSugar.ClickHouse;
using ClickHouseClientNetCoreSqlSugar.Test.Models;
using NUnit.Framework;

namespace ClickHouseClientNetCoreSqlSugar.Test {
    public class ClickHouseSqlSugarUnitTest {

        [Test]
        public void GetSqlSugarClient() {
            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            Assert.IsNotNull(sqlSugarClient);
        }

        [Test]
        public void GetDayCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = DateTime.Parse("2018-07-18 00:00:00");
            var endTime = DateTime.Parse("2018-07-19 00:00:00");

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .Where(t => t.RequestUserID == customerId && t.OperationTime >= startTime && t.OperationTime < endTime)
                                        .Count();

            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 50);
        }

        [Test]
        public void GetHourCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = DateTime.Parse("2018-07-18 08:52:00");
            var endTime = DateTime.Parse("2018-07-18 08:53:00");

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .Where(t => t.RequestUserID == customerId && t.OperationTime >= startTime && t.OperationTime < endTime)
                                        .Count();

            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 6);
        }

        [Test]
        public void GetGroupCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = DateTime.Parse("2018-07-18 00:00:00");
            var endTime = DateTime.Parse("2018-07-19 00:00:00");
            var methods = new[] { "GetSingleRecord", "GetSingleRecordNJBank", "GetArticlesOfLawList" };

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .Where(t => t.RequestUserID == customerId
                                                    && methods.Any(m => m.Equals(t.MethodName, StringComparison.CurrentCultureIgnoreCase))
                                                    && !string.IsNullOrWhiteSpace(library) && t.Library.ToLower() == library.ToLower()
                                                    && t.OperationTime >= startTime
                                                    && t.OperationTime < endTime)
                                        .Select(t => new { t.Library, t.Gid })
                                        .Distinct()
                                        .Count();

            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 8);
        }

        [Test]
        public void GetGroupList() {


            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = DateTime.Parse("2018-07-18 00:00:00");
            var endTime = DateTime.Parse("2018-07-19 00:00:00");
            var methods = new[] { "GetSingleRecord", "GetSingleRecordNJBank", "GetArticlesOfLawList" };
            var pageIndex = 1;
            var pageSize = 5;

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .Where(t => t.RequestUserID == customerId
                                                    && methods.Any(m => m.Equals(t.MethodName, StringComparison.CurrentCultureIgnoreCase))
                                                    && t.Library.ToLower() == library.ToLower()
                                                    && t.OperationTime >= startTime
                                                    && t.OperationTime < endTime)
                                        .Select(t => new SingleRecordLogResult() { Library = t.Library, Gid = t.Gid })
                                        .Distinct()
                                        .ToPageList(pageIndex, pageSize);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.Count == pageSize);
        }

        [Test]
        public void GetAccessLogList() {

            var pageIndex = 1;
            var pageSize = 5;

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .ToPageList(pageIndex, pageSize);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.Count == pageSize);
        }

        [Test]
        public void GetAccessLogInfo() {
            var id = "1";

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();
            var result = sqlSugarClient.Queryable<CustomerAccessLog>()
                                        .Where(t => t.ID == id)
                                        .First();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ID == "1");
        }

        [Test]
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

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();

            var result = sqlSugarClient.Insertable(model).ExecuteCommand();

            Assert.IsTrue(result == 0);
        }

        [Test]
        public void DeleteAccessLog() {

            var id = "1";

            var sqlSugarClient = ClickHouseSqlSugar.GetSqlSugarClient();

            var result = sqlSugarClient.Deleteable<CustomerAccessLog>().Where(t=>t.ID==id).ExecuteCommand();

            Assert.IsTrue(result == 0);
        }
    }
}
