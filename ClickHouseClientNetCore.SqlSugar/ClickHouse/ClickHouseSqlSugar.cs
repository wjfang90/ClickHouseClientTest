using SqlSugar;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace ClickHouseClientNetCore.SqlSugar.ClickHouse {
    /// <summary>
    /// SqlSugarClient ClickHouse only support .net core
    /// 
    /// SqlSugar.ClickHouseCore 依赖 ClickHouse.Client
    /// </summary>
    public class ClickHouseSqlSugar {

        private static string ConnStr = "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=False;Compressor=lz4;Host=192.168.0.123;Port=8123;Database=DbInterface;User=default;Password=2023";
        public static SqlSugarClient GetSqlSugarClient() {

            if (string.IsNullOrWhiteSpace(ConnStr)) return null;

            SqlSugarClient sqlSugarClient = null;
            try {
                var conn = new ConnectionConfig();
                conn.ConnectionString = ConnStr;
                conn.DbType = DbType.ClickHouse;
                conn.IsAutoCloseConnection = true;

                sqlSugarClient = new SqlSugarClient(conn);
                return sqlSugarClient;
            }
            catch (Exception ex) {
                
                if (sqlSugarClient != null)
                    sqlSugarClient.Close();

                return null;
            }
        }
    }
}
