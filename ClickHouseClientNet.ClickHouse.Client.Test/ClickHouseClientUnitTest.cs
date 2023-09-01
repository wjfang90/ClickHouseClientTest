using ClickHouse.Client.ADO.Parameters;
using ClickHouseClientNet.ClickHouse.Client.Test.Models;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;


namespace ClickHouseClientNet.ClickHouse.Client.Test {
    public class ClickHouseClientUnitTest {
        [SetUp]
        public void Setup() {
        }


        [Test]
        public void GetDayCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";

            var count = ClickHouseClientService.GetCount(customerId, startTime, endTime);

            Assert.IsTrue(count > 0);
            Assert.IsTrue(count == 50);
        }

        [Test]
        public void GetHourCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var startTime = "2018-07-18 08:52:00";
            var endTime = "2018-07-19 08:53:00";

            var count = ClickHouseClientService.GetCount(customerId, startTime, endTime);

            Assert.IsTrue(count > 0);
            Assert.IsTrue(count == 6);
        }

        [Test]
        public void GetGroupGidCount() {

            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";

            var count = ClickHouseClientService.GetGroupGidCount(customerId, library, startTime, endTime);

            Assert.IsTrue(count > 0);
            Assert.IsTrue(count == 8);
        }

        [Test]
        public void GetGroupGidList() {
            var customerId = "DZ0Z86HBJ0PB8N2V";
            var library = "pfnl";
            var startTime = "2018-07-18 00:00:00";
            var endTime = "2018-07-19 00:00:00";
            var pageSize = 5;

            var result = ClickHouseClientService.GetGroupGidList(customerId, library, startTime, endTime, pageSize: pageSize);

            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.Count == pageSize);
        }

        [Test]
        public void GetOne() {
            var queryModel = ClickHouseClientService.GetOne("1");
            
            Assert.IsNotNull(queryModel);
            Assert.IsTrue(queryModel.ID == "1");
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

            var result = ClickHouseClientService.AddAccessLog(model);

            Assert.IsTrue(result);
        }

        [Test]
        public void DeleteAccessLog() {

            var id = "1";

            var result = ClickHouseClientService.DeleteAccessLog(id);

            Assert.IsTrue(result);
        }
    }
}