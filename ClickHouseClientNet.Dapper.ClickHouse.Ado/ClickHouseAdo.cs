using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace ClickHouseClientNet.Dapper {
    public class ClickHouseAdo {
        protected static readonly string connection = System.Configuration.ConfigurationManager.ConnectionStrings["ClickhouseConnection"]?.ConnectionString ?? "Host=192.168.0.123;Port=9000;User=default;Password=2023;Database=DbInterface;Compress=True";

        protected static IDbConnection GetConection() {

            if (string.IsNullOrWhiteSpace(connection)) return null;

            ClickHouseConnection conn = null;
            try {
                conn = new ClickHouseConnection(connection);//ok
                conn.Open();

                return conn;
            }
            catch (Exception ex) {
                if (conn != null)
                    conn.Dispose();

                return null;
            }
        }
    }
}
