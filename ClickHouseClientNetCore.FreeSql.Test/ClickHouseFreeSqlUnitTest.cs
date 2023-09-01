using ClickHouseClientNetCore.ClickHouse;
using ClickHouseClientNetCore.Test.Models;
using NUnit.Framework;
using System;
using System.Linq;

namespace ClickHouseClientNetCore.FreeSql.Test{
    public class ClickHouseFreeSqlUnitTest {
        [SetUp]
        public void Setup() {
        }

        [Test]
        public void GetFreeSql() {
            var freeSql = ClickHouseFreeSql.FreeSql;
            Assert.IsNotNull(freeSql);
        }

        [Test]
        public void GetDayCount() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTimeStr = "2018-07-18 00:00:00";
            var endTimeStr = "2018-07-19 00:00:00";
            var startTime = DateTime.Parse(startTimeStr);
            var endTime = DateTime.Parse(endTimeStr);
            var result = ClickHouseFreeSql.FreeSql.Select<CustomerAccessLog>()
                                      .Where(t => t.RequestUserID == customerId && t.OperationTime >= startTime && t.OperationTime < endTime)
                                      .Count();

            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 50);
        }

        [Test]
        public void GetHourCount() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTimeStr = "2018-07-18 08:52:00";
            var endTimeStr = "2018-07-18 08:53:00";
            var startTime = DateTime.Parse(startTimeStr);
            var endTime = DateTime.Parse(endTimeStr);
            var result = ClickHouseFreeSql.FreeSql.Queryable<CustomerAccessLog>()
                                      .Where(t => t.RequestUserID == customerId && t.OperationTime >= startTime && t.OperationTime < endTime)
                                      .Count();
            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 6);
        }

        [Test]
        public void GetGroupCount() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTimeStr = "2018-07-18 00:00:00";
            var endTimeStr = "2018-07-19 00:00:00";
            var methods = new[] { "GetSingleRecord", "GetSingleRecordNJBank", "GetArticlesOfLawList" };
            var startTime = DateTime.Parse(startTimeStr);
            var endTime = DateTime.Parse(endTimeStr);
            var result = ClickHouseFreeSql.FreeSql.Queryable<CustomerAccessLog>()
                                      .Where(t => t.RequestUserID == customerId && t.Library.ToLower() == library.ToLower()
                                                && methods.Any(m=>m.ToLower() == t.MethodName.ToLower())
                                                && t.OperationTime >= startTime && t.OperationTime < endTime)
                                      .GroupBy(t => new { t.Library, t.Gid })
                                      .Count();

            Assert.IsTrue(result > 0);
            Assert.IsTrue(result == 8);
        }

        [Test]
        public void GetGroupList() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var methods = new[] { "GetSingleRecord", "GetSingleRecordNJBank", "GetArticlesOfLawList" };
            var startTimeStr = "2018-07-18 00:00:00";
            var endTimeStr = "2018-07-19 00:00:00";
            var startTime = DateTime.Parse(startTimeStr);
            var endTime = DateTime.Parse(endTimeStr);
            var pageIndex = 1;
            var pageSize = 5;

            var result = ClickHouseFreeSql.FreeSql.Queryable<CustomerAccessLog>()
                                      .Where(t => t.RequestUserID == customerId && t.Library.ToLower() == library.ToLower()
                                                  && methods.Any(m => m.ToLower() == t.MethodName.ToLower())
                                                  && t.OperationTime >= startTime && t.OperationTime < endTime)
                                      .GroupBy(t => new { Library = t.Library, Gid = t.Gid })
                                      .Skip((pageIndex - 1) * pageSize)
                                      .Take(pageSize)
                                      .Select(t => new SingleRecordLogResult() { Library = t.Key.Library, Gid = t.Key.Gid })
                                      .ToList();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.Count == pageSize);

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

            var result = ClickHouseFreeSql.FreeSql.Insert(model).ExecuteAffrows();

            Assert.IsTrue(result == 0);
        }

        [Test]
        public void DeleteAccessLog() {
            var id = "1";

            var result = ClickHouseFreeSql.FreeSql.Delete<CustomerAccessLog>()
                                      .Where(t => t.ID == id)
                                      .ExecuteAffrows();

            Assert.IsTrue(result == 0);
        }
    }
}