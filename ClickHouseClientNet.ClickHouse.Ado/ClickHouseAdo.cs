using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ClickHouseClientNet.ClickHouse.Ado {
    /// <summary>
    /// ClickHouse.Ado
    /// 最低支持 .NETFramework 4.5.1
    /// 支持 .net core
    /// </summary>
    public class ClickHouseAdo {
        protected static readonly string connection = System.Configuration.ConfigurationManager.ConnectionStrings["ClickhouseConnection"]?.ConnectionString ?? "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=False;Compressor=lz4;Host=192.168.0.123;Port=9000;Database=DbInterface;User=default;Password=2023";

        public static object ExecuteScalar(string sql, params ClickHouseParameter[] parameters) {

            if (string.IsNullOrWhiteSpace(sql)) return null;

            try {
                using (var conn = GetConection()) {

                    using (var cmd = conn.CreateCommand(sql)) {

                        if (parameters != null && parameters.Any()) {
                            parameters.ToList().ForEach(t => {
                                cmd.Parameters.Add(t);
                            });
                        }

                        var result = cmd.ExecuteScalar();//ok
                        return result;
                    }
                }
            }
            catch (Exception ex) {

                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static IEnumerable<T> QueryList<T>(string sql, params ClickHouseParameter[] parameters) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {
                using (var conn = GetConection()) {

                    using (var command = new ClickHouseCommand(conn, sql)) {

                        if (parameters != null && parameters.Any()) {
                            parameters.ToList().ForEach(t => {
                                command.Parameters.Add(t);
                            });
                        }

                        using (var reader = command.ExecuteReader()) {//ok

                            var modelList = new List<T>();

                            reader.ReadAll(
                                rowReader => {
                                    var model = Activator.CreateInstance<T>();

                                    for (int i = 0; i < rowReader.FieldCount; i++) {
                                        var name = rowReader.GetName(i);
                                        var value = rowReader.GetValue(i);

                                        if (model.GetType().GetProperty(name).PropertyType == typeof(DateTime)) {
                                            value = TimeZoneInfo.ConvertTimeFromUtc(Convert.ToDateTime(value), TimeZoneInfo.Local);//将UTC时间转换为本地时间。clickhouse 中数据存储为北京时间，用ClickHouse.Ado 取到的时间为 UTC时间格式，但时间被加了8个小时
                                        }
                                        model.GetType().GetProperty(name).SetValue(model, value);
                                    }
                                    modelList.Add(model);
                                }
                            );
                            return modelList;
                        }
                    }
                }
            }
            catch (Exception ex) {

                return default;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>        
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static T QueryOne<T>(string sql, params ClickHouseParameter[] parameters) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {
                using (var conn = GetConection()) {

                    using (var command = new ClickHouseCommand(conn, sql)) {

                        if (parameters != null && parameters.Any()) {
                            parameters.ToList().ForEach(t => {
                                command.Parameters.Add(t);
                            });
                        }

                        using (var reader = command.ExecuteReader()) {//ok

                            T model = Activator.CreateInstance<T>();

                            if (reader.NextResult()) {
                                if (reader.Read()) {

                                    for (int i = 0; i < reader.FieldCount; i++) {
                                        var name = reader.GetName(i);
                                        var value = reader.GetValue(i);

                                        if (typeof(T).GetProperty(name).PropertyType == typeof(DateTime)) {
                                            value = TimeZoneInfo.ConvertTimeFromUtc(Convert.ToDateTime(value), TimeZoneInfo.Local);//将UTC时间转换为本地时间。
                                        }
                                        model.GetType().GetProperty(name).SetValue(model, value);
                                    }
                                }
                            }


                            return model;
                        }
                    }
                }
            }
            catch (Exception ex) {

                return default;
            }
        }


        public static bool Add(string sql, params ClickHouseParameter[] parameters) {
            return ExecuteNoQuery(sql, parameters);
        }

        public static bool Delete(string sql, params ClickHouseParameter[] parameters) {
            return ExecuteNoQuery(sql, parameters);
        }

        protected static bool ExecuteNoQuery(string sql, params ClickHouseParameter[] parameters) {
            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {
                using (var conn = GetConection()) {

                    using (var command = new ClickHouseCommand(conn, sql)) {

                        if (parameters != null && parameters.Any()) {
                            parameters.ToList().ForEach(t => {
                                command.Parameters.Add(t);
                            });
                        }

                        var result = command.ExecuteNonQuery();
                        return result == 0;
                    }
                }
            }
            catch (Exception ex) {

                return default;
            }
        }

        protected static ClickHouseConnection GetConection() {

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
