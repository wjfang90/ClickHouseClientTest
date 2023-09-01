using ClickHouse.Client.ADO.Parameters;
using ClickHouseClientNet.ClickHouse.Client.Test.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseClientNet.ClickHouse.Client.Test {
   public class ClickHouseClientService {
        public static ulong GetCount(string customerId, string startTime,string endTime) {
            var sql = @"select count(1) from DbInterface.CustomerAccessLog 
                      where RequestUserID=@customerId and OperationTime>=@startTime and OperationTime<@endTime ";

            var paramList = new List<ClickHouseDbParameter>() {
                 new ClickHouseDbParameter() {
                     ParameterName="customerId",
                     Value=customerId,
                     DbType=DbType.String
                 },
                new ClickHouseDbParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                },
                new ClickHouseDbParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                }
             };
            var count = ClickHouseClient.ExecuteScalar<ulong>(sql, paramList.ToArray());
            return count;
        }

        public static ulong GetGroupGidCount(string customerId, string library, string startTime = null, string endTime = null) {

            var sql = @"select count(1) as listCount from (
										select distinct Library,Gid from DbInterface.CustomerAccessLog l 
										where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord'  or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
									) t";

            var paramsList = new List<ClickHouseDbParameter>() {
               new ClickHouseDbParameter() {
                   ParameterName="customerId",
                   Value=customerId,
                   DbType=DbType.String
               }               
            };

            if (!string.IsNullOrWhiteSpace(library)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "library",
                    Value = library,
                    DbType = DbType.String
                });
            }

            if (!string.IsNullOrWhiteSpace(startTime)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                });
            }
            if (!string.IsNullOrWhiteSpace(endTime)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                });
            }

            var count = ClickHouseClient.ExecuteScalar<ulong>(sql, paramsList.ToArray());

            return count;
        }

        public static List<SingleRecordLogResult> GetGroupGidList(string customerId, string library, string startTime = null, string endTime = null,int pageIndex=1,int pageSize=10) {

            var sql = @"select * from (
                        select distinct Library,Gid from DbInterface.CustomerAccessLog l
						where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord' or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
                        ) t 
                        limit @pageSize offset (@pageIndex-1)*@pageSize";

            var paramsList = new List<ClickHouseDbParameter>() {
               new ClickHouseDbParameter() {
                   ParameterName="customerId",
                   Value=customerId,
                   DbType=DbType.String
               },
               new ClickHouseDbParameter() {
                   ParameterName="pageIndex",
                   Value=pageIndex,
                   DbType=DbType.Int32
               },
                new ClickHouseDbParameter() {
                   ParameterName="pageSize",
                   Value=pageSize,
                   DbType=DbType.Int32
               }
            };

            if (!string.IsNullOrWhiteSpace(library)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "library",
                    Value = library,
                    DbType = DbType.String
                });
            }

            if (!string.IsNullOrWhiteSpace(startTime)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "startTime",
                    Value = Convert.ToDateTime(startTime),
                    DbType = DbType.DateTime
                });
            }
            if (!string.IsNullOrWhiteSpace(endTime)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = "endTime",
                    Value = Convert.ToDateTime(endTime),
                    DbType = DbType.DateTime
                });
            }

            var result = ClickHouseClient.QueryList<SingleRecordLogResult>(sql, paramsList.ToArray()).ToList();

            return result;
        }


        public static CustomerAccessLog GetOne(string Id) {
            var sql = @"select ID,Library,RequestUserID,OperationTime from DbInterface.CustomerAccessLog where ID=@id";
            var param = new ClickHouseDbParameter() {
                ParameterName = "id",
                Value = Id,
                DbType = DbType.String
            };
            var result = ClickHouseClient.Query<CustomerAccessLog>(sql, param);

            return result;
        }

        public static bool AddAccessLog(CustomerAccessLog logInfo) {

            var sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,RequestIP,RequestUserID,OperationTime {0}) values (@ID,@MethodName,@RequestIP,@RequestUserID,@OperationTime {1})";
            var dict = new Dictionary<string, string>();

            var paramsList = new List<ClickHouseDbParameter>() {

               new ClickHouseDbParameter() {
                   ParameterName=nameof(CustomerAccessLog.ID),
                   Value=logInfo.ID,
                   DbType=DbType.String
               },
               new ClickHouseDbParameter() {
                   ParameterName=nameof(CustomerAccessLog.MethodName),
                   Value=logInfo.MethodName,
                   DbType=DbType.String
               },
               new ClickHouseDbParameter() {
                   ParameterName=nameof(CustomerAccessLog.RequestIP),
                   Value=logInfo.RequestIP,
                   DbType=DbType.String
               },
                new ClickHouseDbParameter() {
                   ParameterName = nameof(CustomerAccessLog.RequestUserID),
                   Value=logInfo.RequestUserID,
                   DbType=DbType.String
               },
                new ClickHouseDbParameter() {
                   ParameterName=nameof(CustomerAccessLog.OperationTime),
                   Value=DateTime.Now,
                   DbType=DbType.DateTime
               }
            };

            if (!string.IsNullOrWhiteSpace(logInfo.Library)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = nameof(CustomerAccessLog.Library),
                    Value = logInfo.Library,
                    DbType = DbType.String
                });
                dict.Add(nameof(CustomerAccessLog.Gid), nameof(CustomerAccessLog.Gid));
            }

            if (!string.IsNullOrWhiteSpace(logInfo.Gid)) {
                paramsList.Add(new ClickHouseDbParameter() {
                    ParameterName = nameof(CustomerAccessLog.Gid),
                    Value = logInfo.Gid,
                    DbType = DbType.String
                });
                dict.Add(nameof(CustomerAccessLog.Library), nameof(CustomerAccessLog.Library));
            }
            if (!string.IsNullOrWhiteSpace(logInfo.ParameterList)) {
                paramsList.Add(new ClickHouseDbParameter() {
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

            var result = ClickHouseClient.ExecuteNonQuery(sql, paramsList.ToArray());

            return result > 0;
        }


        public static bool DeleteAccessLog(string id) {
            var sql = "delete from DbInterface.CustomerAccessLog where ID=@id";

            var paramsList = new List<ClickHouseDbParameter>() {
               new ClickHouseDbParameter() {
                   ParameterName="id",
                   Value=id,
                   DbType=DbType.String
               }
            };
            var result = ClickHouseClient.ExecuteNonQuery(sql, paramsList.ToArray());
            return result > 0;
        }
    }
}
