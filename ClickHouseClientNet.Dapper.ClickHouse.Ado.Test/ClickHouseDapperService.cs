using ClickHouseClientNet.Dapper.Test.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseClientNet.Dapper.Test {
    public class ClickHouseDapperService {
        public static int GetCount(string customerId, string startTime, string endTime) {
            var sql = @"select count(1) from DbInterface.CustomerAccessLog 
                      where RequestUserID=@customerId and OperationTime>=@startTime and OperationTime<@endTime";
            var param = new { customerId = customerId, startTime = startTime, endTime = endTime };
            var count = ClickHouseDapper.ExecuteScalar<int>(sql, param);
            return count;
        }

        public static int GetGroupGidCount(string customerId, string library, string startTime = null, string endTime = null) {
            var sql = @"select count(1) as listCount from (
										select distinct Library,Gid from DbInterface.CustomerAccessLog l 
										where RequestUserID=@customerId and (LOWER(MethodName) ='getsinglerecord'  or LOWER(MethodName) ='getsinglerecordnjbank' or LOWER(MethodName)='getarticlesoflawlist') 
                                              and LOWER(Library)=@library and OperationTime>=@startTime and OperationTime<@endTime
									) t";
            library = library.ToLower();


            object param;

            if (!string.IsNullOrWhiteSpace(library) && !string.IsNullOrWhiteSpace(startTime) && !string.IsNullOrWhiteSpace(endTime)) {
                param = new { customerId = customerId, library = library, startTime = startTime, endTime = endTime };
            }
            else if (!string.IsNullOrWhiteSpace(library) && !string.IsNullOrWhiteSpace(startTime)) {
                param = new { customerId = customerId, library = library, startTime = startTime };
            }
            else if (!string.IsNullOrWhiteSpace(library) && !string.IsNullOrWhiteSpace(endTime)) {
                param = new { customerId = customerId, library = library, endTime = endTime };
            }
            else if (startTime != null && !string.IsNullOrWhiteSpace(endTime)) {
                param = new { customerId = customerId, startTime = startTime, endTime = endTime };
            }
            else if (!string.IsNullOrWhiteSpace(library)) {
                param = new { customerId = customerId, library = library };
            }
            else if (!string.IsNullOrWhiteSpace(startTime)) {
                param = new { customerId = customerId, startTime = startTime };
            }
            else if (!string.IsNullOrWhiteSpace(endTime)) {
                param = new { customerId = customerId, endTime = endTime };
            }
            else {
                param = new { customerId = customerId };
            }

            var groupGidCount = ClickHouseDapper.ExecuteScalar<int>(sql, param);
            return groupGidCount;
        }

        public static List<SingleRecordLogResult> GetGroupGidList(string customerId, string library, string startTimeStr = null, string endTimeStr = null, int pageIndex = 1, int pageSize = 10) {
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

            object param;

            if (!string.IsNullOrWhiteSpace(library) && startTimeStr != null && endTimeStr != null) {
                param = new { customerId = customerId, library = library, startTime = startTimeStr, endTime = endTimeStr, pageIndex = pageIndex, pageSize = pageSize };
                //param = new { customerId = customerId, library = library, startTime = DateTime.Parse(startTimeStr), endTime = DateTime.Parse(endTimeStr), pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (!string.IsNullOrWhiteSpace(library) && startTimeStr != null) {
                param = new { customerId = customerId, library = library, startTime = startTimeStr, pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (!string.IsNullOrWhiteSpace(library) && endTimeStr != null) {
                param = new { customerId = customerId, library = library, endTime = endTimeStr, pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (startTimeStr != null && endTimeStr != null) {
                param = new { customerId = customerId, startTime = startTimeStr, endTime = endTimeStr, pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (!string.IsNullOrWhiteSpace(library)) {
                param = new { customerId = customerId, library = library, pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (startTimeStr != null) {
                param = new { customerId = customerId, startTime = startTimeStr, pageIndex = pageIndex, pageSize = pageSize };
            }
            else if (endTimeStr != null) {
                param = new { customerId, endTime = endTimeStr, pageIndex = pageIndex, pageSize = pageSize };
            }
            else {
                param = new { customerId, pageIndex, pageSize };
            }

            var groupList = ClickHouseDapper.Query<SingleRecordLogResult>(sql, param).ToList();

            return groupList;
        }

        public static CustomerAccessLog GetOne(string id) {
            var sql = @"select * from DbInterface.CustomerAccessLog where ID=@id limit 1";

            var result = ClickHouseDapper.QueryFirstOrDefault<CustomerAccessLog>(sql, new { id });

            return result;
        }

        public static bool Add(CustomerAccessLog model) {
            var sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,ParameterList,Library,Gid,RequestIP,RequestUserID,OperationTime) values (@ID,@MethodName,@ParameterList,@Library,@Gid,@RequestIP,@RequestUserID,@OperationTime)";//ok
            //sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,ParameterList,Library,Gid,RequestIP,RequestUserID,OperationTime) select @ID,@MethodName,@ParameterList,@Library,@Gid,@RequestIP,@RequestUserID,@OperationTime";//ok
            //sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,ParameterList,Library,Gid,RequestIP,RequestUserID,OperationTime) values (@ID,@MethodName,@ParameterList,@Library,@Gid,@RequestIP,@RequestUserID,now('Asia/Shanghai'))";//ok 
            //sql = "insert into DbInterface.CustomerAccessLog(ID,MethodName,ParameterList,Library,Gid,RequestIP,RequestUserID,OperationTime) values (@ID,@MethodName,@ParameterList,@Library,@Gid,@RequestIP,@RequestUserID,now('Asia/Shanghai') + INTERVAL 8 HOUR)";//ok 

            var result = ClickHouseDapper.Add(sql, model);

            Console.WriteLine($"add ={result}");

            return result;

        }

        public static bool Delete(string id) {
            var sql = "delete from DbInterface.CustomerAccessLog where ID=@id";

            var result = ClickHouseDapper.Delete(sql, new { id });
            return result;
        }

        public static dynamic GetOnePeople(int id) {
            var sql = "select * from DbInterface.people where id=@id limit 1";
            sql = "select * from DbInterface.people where id=@id";
            var res = ClickHouseDapper.QueryFirstOrDefault(sql, new { id });
            return res;
        }
    }
}
