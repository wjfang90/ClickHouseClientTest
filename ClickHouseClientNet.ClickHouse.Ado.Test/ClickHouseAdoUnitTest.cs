using ClickHouseClientNet.ClickHouse.Ado.Test.Models;
using NUnit.Framework;
using System;
using System.Linq;

namespace ClickHouseClientNet.ClickHouse.Ado.Test {
    public class ClickHouseAdoUnitTest {
        [SetUp]
        public void Setup() {
        }

        [Test]
        public void GetDayCount() {
            var count = ClickHouseAdoService.GetCount("DZ0Z86HBJ0PB8N2V", "2018-07-18 00:00:00", "2018-07-19 00:00:00"); //ok
            Assert.IsTrue(count > 0);
            Assert.AreEqual(50, count);
        }

        [Test]
        public void GetHourCount() {
            var count = ClickHouseAdoService.GetCount("DZ0Z86HBJ0PB8N2V", "2018-07-18 08:52:00", "2018-07-18 08:53:00"); //ok
            Assert.IsTrue(count > 0);
            Assert.AreEqual(6, count);
        }

        [Test]
        public void GetGroupGidCount() {
            var count = ClickHouseAdoService.GetGroupGidCount("DZ0Z86HBJ0PB8N2V", "pfnl", "2018-07-18 00:00:00", "2018-07-19 00:00:00"); //ok
            Assert.IsTrue(count > 0);
            Assert.AreEqual(8, count);
        }

        [Test]
        public void GetGroupGidList() {
            var pageSize = 5;
            var result = ClickHouseAdoService.GetGroupGidList("DZ0Z86HBJ0PB8N2V", "pfnl", "2018-07-18 00:00:00", "2018-07-19 00:00:00", pageSize: pageSize); //ok
            Assert.IsTrue(result.ToList().Count > 0);
            Assert.AreEqual(pageSize, result.ToList().Count);
        }

        [Test]
        public void GetOne() {
            var id = "1";
            var result = ClickHouseAdoService.GetOne(id);//ok
            Assert.IsNotNull(result);
            Assert.AreEqual(id, result.ID);
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
                RequestUserID = "test customerid",
                OperationTime = DateTime.UtcNow // 注意使用UTC时间
            };

            var result = ClickHouseAdoService.Add(addModel);

            Assert.IsTrue(result);
        }

        [Test]
        public void DeleteAccessLog() {
            var deleteModel = new CustomerAccessLog() {
                ID = "1",
                MethodName = "test methodname",
                ParameterList = "test parameters",
                Library = "test library",
                Gid = "test gid",
                RequestIP = "192.168.4.179",
                RequestUserID = "test customerid",
                OperationTime = DateTime.Now
            };

            var result = ClickHouseAdoService.Delete(deleteModel);//ok
            Assert.IsTrue(result);
        }
    }
}