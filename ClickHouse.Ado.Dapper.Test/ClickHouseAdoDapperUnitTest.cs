using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Data;

namespace UnitTestProject1 {
    /// <summary>
    /// nuget 安装 ClickHouse.Ado.Dapper 1.0.10
    /// </summary>
    [TestClass]
    public class ClickHouseAdoDapperUnitTest {
        [TestMethod]
        public void TestMethod1() {

            var connStr = "Host=192.168.0.123;Port=9000;User=default;Password=2023;Database=DbInterface;Compress=True";
            IDbConnection conn = new ClickHouse.Ado.ClickHouseConnection(connStr);
            conn.Open();
            var data1 = conn.Query("select * from people limit 3");
            var data = conn.Query("select * from people where id=@id limit 1", new { id = 8 });

            Assert.IsNotNull(data1);
            Assert.AreEqual(3, data1.ToList().Count);
            Assert.IsNotNull(data);
            Assert.AreEqual(8, data.First().id);
        }
    }
}
