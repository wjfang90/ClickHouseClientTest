using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouseClientNet.Dapper {
    /// <summary>
    /// Dapper 2.0.143 execute Cannot parse string '2023/7/18 15:35:49' as DateTime:
    /// Dapper 2.0.138 Query error The provided reader is required to be a DbDataReader, execute Cannot parse string '2023/7/18 15:35:49' as DateTime:
    /// Dapper 2.0.123 execute Cannot parse string '2023/7/18 15:35:49' as DateTime:
    /// Dapper 2.0.90 error
    /// 
    /// 支持 .NETFramework 4.6.1
    /// 支持 .net core
    /// 依赖 ClickHouse.Ado
    /// </summary>
    public class ClickHouseDapper:ClickHouseAdo {
        public static T ExecuteScalar<T>(string sql, object parameter) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {

                using (var conn = GetConection()) {
                    var result = conn.ExecuteScalar<T>(sql, parameter);
                    return result;
                }
            }
            catch (Exception ex) {

                return default;
            }
        }

        public static bool Add(string sql, object papameter) {
            return ExecuteNonQuery(sql, papameter);
        }

        public static bool Delete(string sql, object papameter) {
            return ExecuteNonQuery(sql, papameter);
        }



        public static IEnumerable<dynamic> Query(string sql, object parameter) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {

                var conn = GetConection();

                return conn.Query(sql, parameter);//failure

            }
            catch (Exception ex) {

                return default;
            }
        }

        public static IEnumerable<T> Query<T>(string sql, object parameter) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {

                var conn = GetConection();
                var res = conn.Query<T>(sql, parameter);//failure
                return res;
            }
            catch (Exception ex) {

                return default;
            }
        }

        public static T QueryFirstOrDefault<T>(string sql, object parameter) {

            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {

                var conn = GetConection();

                var result = conn.QueryFirstOrDefault<T>(sql, parameter);//failure
                return result;
            }
            catch (Exception ex) {

                return default;
            }
        }

        public static dynamic QueryFirstOrDefault(string sql, object parameter) {
            try {
                var conn = GetConection();
                var result = conn.QueryFirstOrDefault(sql, parameter);//failure
                //var result = conn.QuerySingleOrDefault(sql, parameter);         //failure       
                return result;
            }
            catch (Exception ex) {

                return default;
            }
        }

        protected static bool ExecuteNonQuery(string sql, object parameter) {
            if (string.IsNullOrWhiteSpace(sql)) return default;

            try {

                using (var conn = GetConection()) {
                    var result = conn.Execute(sql, parameter);//Cannot convert parameter with type AnsiString to DateTime.
                    return result == 0;
                }
            }
            catch (Exception ex) {

                return default;
            }
        }
    }
}
