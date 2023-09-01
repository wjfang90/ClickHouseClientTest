using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Adapters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseClientNet.ClickHouse.Client {
    /// <summary>
    /// ClickHouse.Client
    /// 最低版本 .NETFramework 4.6.2
    /// 支持 .net core
    /// </summary>
    public class ClickHouseClient {

        private static string ConnStr => "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=False;Compressor=lz4;Host=192.168.0.123;Port=8123;Database=DbInterface;User=default;Password=2023";

        public static ClickHouseConnection GetConnection() {
            var conn = new ClickHouseConnection(ConnStr);
            conn.Open();
            return conn;
        }

        public static T ExecuteScalar<T>(string sql, params DbParameter[] parameters) {
            using (var conn = GetConnection()) {
                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = sql;
                    cmd.Parameters.AddRange(parameters);

                    var res = cmd.ExecuteScalar();
                    var result = (T)res;
                    return result;
                }
            }
        }

        public static IEnumerable<T> QueryList<T>(string sql, params DbParameter[] parameters) {
            using (var conn = GetConnection()) {
                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = sql;
                    cmd.Parameters.AddRange(parameters);

                    var result = new List<T>();

                    using (var adapter = new ClickHouseDataAdapter()) {
                        adapter.SelectCommand = cmd;

                        var dataTable = new DataTable();
                        adapter.Fill(dataTable);

                        foreach (DataRow rowItem in dataTable.Rows) {

                            var model = Activator.CreateInstance<T>();

                            foreach (DataColumn columnItem in dataTable.Columns) {
                                var value = rowItem[columnItem];

                                model.GetType().GetProperty(columnItem.ColumnName).SetValue(model, value);
                            }

                            result.Add(model);
                        }
                    }

                    return result;
                }
            }
        }

        public static T Query<T>(string sql, params DbParameter[] parameters) {
            
            using (var conn = GetConnection()) {
                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = sql;
                    cmd.Parameters.AddRange(parameters);

                    using (var dbDataReader = cmd.ExecuteReader()) {

                        var model = Activator.CreateInstance<T>();

                        while (dbDataReader.Read()) {

                            for (int i = 0; i < dbDataReader.FieldCount; i++) {
                                var name = dbDataReader.GetName(i);
                                var value = dbDataReader.GetValue(i);
                                model.GetType().GetProperty(name).SetValue(model, value);
                            } 

                            break;
                        }
                        return model;
                    }
                }
            }
        }

        public static int ExecuteNonQuery(string sql,params DbParameter[] parameters) {
            using (var conn = GetConnection()) {
                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = sql;
                    cmd.Parameters.AddRange(parameters);

                    var result = cmd.ExecuteNonQuery();

                    return result;
                }
            }
        }
    }
}
