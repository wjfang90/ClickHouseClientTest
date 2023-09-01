using FreeSql;
using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouseClientNetCore.ClickHouse {
    /// <summary>
    /// FreeSql 支持net framework 4.0+
    /// FreeSql.Provider.ClickHouse 支持 Net core 3.0+
    /// FreeSql.Provider.ClickHouse 依赖 ClickHouse.Client ，使用Http协议,默认端口 8123
    /// </summary>
    public class ClickHouseFreeSql {

        private static string ConnStr => "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=False;Compressor=lz4;Host=192.168.0.123;Port=8123;Database=DbInterface;User=default;Password=2023";
        public static IFreeSql FreeSql => new FreeSqlBuilder().UseConnectionString(DataType.ClickHouse, ConnStr).Build();
    }
}
