using ClickHouse.Client.ADO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace ClickHouseClientNet.ClickHouse.Client.Dapper.Test {
    /// <summary>
    /// Dapper 2.0.90
    /// ClickHouse.Client 6.7.1   net framework 4.6.2+   net core 2.2+
    /// 
    /// </summary>
    public class ClickHouseClientDapper {

        private static string ConnStr => "Host=192.168.0.123;Port=8123;User=default;Password=2023;Database=DbInterface;Compress=True";


        public static int Add<T>(T model) {

            var tableName = typeof(T).Name;
            var fields = string.Join(",", typeof(T).GetProperties().Where(t => model.GetType().GetProperty(t.Name).GetValue(model) != null).Select(t => t.Name));
            var paramNames = string.Join(",", typeof(T).GetProperties().Where(t => model.GetType().GetProperty(t.Name).GetValue(model) != null).Select(t => "@" + t.Name));
            var sql = $"insert into {tableName}({fields}) values({paramNames})";

            var conn = GetConnection();
            var result = conn.Execute(sql, model);
            return result;
        }

        public static ClickHouseConnection GetConnection() {
            var conn = new ClickHouseConnection(ConnStr);
            conn.Open();
            return conn;
        }
    }
}
