using ClickHouseClientNet.Dapper.Test.Models;
using NUnit.Framework;
using System;

namespace ClickHouseClientNet.Dapper.Test {
    public class ClickHouseDapperUintTest {
        [SetUp]
        public void Setup() {
           
        }

        [Test]
        public void GetDayCount() {
            var count = ClickHouseDapperService.GetCount("DZ0Z86HBJ0PB8N2V", "2018-07-18 00:00:00", "2018-07-19 00:00:00"); //ok
            Assert.IsTrue(count > 0);//ok
            Assert.AreEqual(50, count);//ok
        }

        [Test]
        public void GetHourCount() {
            var count = ClickHouseDapperService.GetCount("DZ0Z86HBJ0PB8N2V", "2018-07-18 08:52:00", "2018-07-18 08:53:00"); //ok
            Assert.IsTrue(count > 0);//ok
            Assert.AreEqual(6, count);//ok
        }

        [Test]
        public void GetGroupGidCount() {
            var count = ClickHouseDapperService.GetGroupGidCount("DZ0Z86HBJ0PB8N2V","pfnl", "2018-07-18 00:00:00", "2018-07-19 00:00:00"); //ok
            Assert.IsTrue(count > 0);//ok
            Assert.AreEqual(8, count);//ok
        }

        [Test]
        public void GetGroupGidList() {
            var pageSize = 5;
            var result = ClickHouseDapperService.GetGroupGidList("DZ0Z86HBJ0PB8N2V", "pfnl", "2018-07-18 00:00:00", "2018-07-19 00:00:00", pageSize: pageSize); //ok
            Assert.IsTrue(result.Count > 0);//error result.Count=0
            Assert.AreEqual(pageSize, result.Count);//error
        }

        [Test]
        public void GetOne() {
            var id = "1";
            var result = ClickHouseDapperService.GetOne(id);
            Assert.IsNotNull(result);//error result = null
            Assert.AreEqual(id, result.ID);
        }

        [Test]
        public void GetOnePeople() {
            var id = 8;
            var result = ClickHouseDapperService.GetOnePeople(id);
            Assert.IsNotNull(result);//error result = null
            Assert.AreEqual(id, result.id);
        }

        [Test]
        public void AddAccessLog() {
            var addModel = new CustomerAccessLog() {
                ID = "1",
                MethodName = "test methodname",
                ParameterList = "test parameters",
                Library = "test library",
                Gid = "test gid",
                RequestIP = "192.168.4.179",
                RequestUserID = "customerid",
                OperationTime = DateTime.UtcNow
            };

            var result = ClickHouseDapperService.Add(addModel);//ok

            Assert.IsTrue(result);
        }

        [Test]
        public void DeleteAccessLog() {
            var id = "1";

            var result = ClickHouseDapperService.Delete(id);//ok
            Assert.IsTrue(result);
        }
    }
}