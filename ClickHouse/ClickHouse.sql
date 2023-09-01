CREATE DATABASE IF NOT EXISTS DbInterface;

use DbInterface;

create table CustomerAccessLog
(
ID String,
MethodName String,
ParameterList String,
Library String ,
Gid String ,
RequestIP String ,
RequestUserID String ,
RequestUserName	String ,
OperationTime DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(OperationTime)
ORDER BY (RequestUserID, OperationTime, MethodName)