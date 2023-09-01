using ClickHouse.Ado;
using ClickHouseClientNet.ClickHouse.Ado.Test.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseClientNet.ClickHouse.Ado.Test {
    public class ClickHouseAdoService {
        public static ulong GetCount(string customerId, string startTime, string endTime) {
            var sql = @"select count(1) from DbInterface.CustomerAccessLog 
                      where RequestUserID=@customerId and OperationTime>=@startTime and OperationTime<@endTime ";

            var paramList = new List<ClickHouseParameter>() {
                 new ClickHouseParameter() {
                     ParameterName = "customerId",
                     Value = customerId,
                     DbType = DbType.String
                 },
                new ClickHouseParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                },
                new ClickHouseParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                }
             };
            var hourCount = ClickHouseAdo.ExecuteScalar(sql, paramList.ToArray());
            return (ulong)hourCount;
        }

        public static ulong GetGroupGidCount(string customerId, string library, string startTime = null, string endTime = null) {
            var sql = @"select count(1) from (
                        select distinct Library,Gid from DbInterface.CustomerAccessLog l
						where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord' or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
                        ) t ";

            library = library.ToLower();

            var paramsList = new List<ClickHouseParameter>() {
               new ClickHouseParameter() {
                   ParameterName = "customerId",
                   Value = customerId,
                   DbType = DbType.String
               }
            };

            if (!string.IsNullOrWhiteSpace(library)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "library",
                    Value = library,
                    DbType = DbType.String
                });
            }

            if (!string.IsNullOrWhiteSpace(startTime)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                });
            }
            if (!string.IsNullOrWhiteSpace(endTime)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                });
            }

            var groupCount = ClickHouseAdo.ExecuteScalar(sql, paramsList.ToArray()); //ok

            return (ulong)groupCount;
        }

        public static IEnumerable<SingleRecordLogResult> GetGroupGidList(string customerId, string library, string startTime = null, string endTime = null, int pageIndex = 1, int pageSize = 10) {
            var sql = @"select * from (
                        select distinct Library,Gid from DbInterface.CustomerAccessLog l
						where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord' or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
                        ) t 
                        limit @pageSize offset (@pageIndex-1)*@pageSize";

            library = library.ToLower();
            if (pageIndex <= 0) {
                pageIndex = 1;
            }

            var paramsList = new List<ClickHouseParameter>() {
               new ClickHouseParameter() {
                   ParameterName = "customerId",
                   Value = customerId,
                   DbType = DbType.String
               },
               new ClickHouseParameter() {
                   ParameterName = "pageIndex",
                   Value = pageIndex,
                   DbType = DbType.Int32
               },
                new ClickHouseParameter() {
                   ParameterName = "pageSize",
                   Value = pageSize,
                   DbType = DbType.Int32
               }
            };

            if (!string.IsNullOrWhiteSpace(library)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "library",
                    Value = library,
                    DbType = DbType.String
                });
            }

            if (!string.IsNullOrWhiteSpace(startTime)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                });
            }
            if (!string.IsNullOrWhiteSpace(endTime)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                });
            }

            var groupList = ClickHouseAdo.QueryList<SingleRecordLogResult>(sql, paramsList.ToArray()); //ok

            return groupList;
        }

        public static CustomerAccessLog GetOne(string Id) {
            var sql = @"select ID,Library,RequestUserID,OperationTime from DbInterface.CustomerAccessLog where ID=@id";
            var param = new ClickHouseParameter() {
                ParameterName = "id",
                Value = Id,
                DbType = DbType.String
            };
            var result = ClickHouseAdo.QueryOne<CustomerAccessLog>(sql, param);

            return result;
        }

        public static bool Add(CustomerAccessLog logInfo) {

            var sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,RequestIP,RequestUserID,OperationTime {0}) values (@ID,@MethodName,@RequestIP,@RequestUserID,@OperationTime {1})";
            var dict = new Dictionary<string, string>();

            var paramsList = new List<ClickHouseParameter>() {

               new ClickHouseParameter() {
                   ParameterName = nameof(CustomerAccessLog.ID),
                   Value = logInfo.ID,
                   DbType = DbType.String
               },
               new ClickHouseParameter() {
                   ParameterName = nameof(CustomerAccessLog.MethodName),
                   Value = logInfo.MethodName,
                   DbType = DbType.String
               },
               new ClickHouseParameter() {
                   ParameterName = nameof(CustomerAccessLog.RequestIP),
                   Value = logInfo.RequestIP,
                   DbType = DbType.String
               },
                new ClickHouseParameter() {
                   ParameterName = nameof(CustomerAccessLog.RequestUserID),
                   Value = logInfo.RequestUserID,
                   DbType = DbType.String
               },
                new ClickHouseParameter() {
                   ParameterName = nameof(CustomerAccessLog.OperationTime),
                   Value = logInfo.OperationTime,
                   DbType = DbType.DateTime
               }
            };

            if (!string.IsNullOrWhiteSpace(logInfo.Library)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = nameof(CustomerAccessLog.Library),
                    Value = logInfo.Library,
                    DbType = DbType.String
                });
                dict.Add(nameof(CustomerAccessLog.Library), nameof(CustomerAccessLog.Library));
            }

            if (!string.IsNullOrWhiteSpace(logInfo.Gid)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = nameof(CustomerAccessLog.Gid),
                    Value = logInfo.Gid,
                    DbType = DbType.String
                });
                dict.Add(nameof(CustomerAccessLog.Gid), nameof(CustomerAccessLog.Gid));
            }
            if (!string.IsNullOrWhiteSpace(logInfo.ParameterList)) {
                paramsList.Add(new ClickHouseParameter() {
                    ParameterName = nameof(CustomerAccessLog.ParameterList),
                    Value = logInfo.ParameterList,
                    DbType = DbType.String
                });
                dict.Add(nameof(CustomerAccessLog.ParameterList), nameof(CustomerAccessLog.ParameterList));
            }

            var fields = string.Empty;
            var parameters = string.Empty;
            if (dict.Any()) {
                fields = "," + string.Join(",", dict.Keys.ToArray());
                parameters = "," + string.Join(",", dict.Values.Select(t => "@" + t).ToArray());
            }

            sql = string.Format(sql, fields, parameters);

            var result = ClickHouseAdo.Add(sql, paramsList.ToArray());
            return result;
        }


        public static bool Delete(CustomerAccessLog logInfo) {
            var sql = "delete from DbInterface.CustomerAccessLog where ID=@id";

            var paramsList = new List<ClickHouseParameter>() {
               new ClickHouseParameter() {
                   ParameterName = "id",
                   Value = logInfo.ID,
                   DbType = DbType.String
               }
            };
            var result = ClickHouseAdo.Delete(sql, paramsList.ToArray());
            return result;
        }
    }
}
