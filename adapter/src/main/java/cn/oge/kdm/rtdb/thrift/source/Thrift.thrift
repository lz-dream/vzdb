struct TRtValue{
	1:i64 time,
	2:string value,
	
}
struct TRtSnapshotData{
    1:string code,
    2:string state,
	3:TRtValue data,
}
struct TRtHistoryData{
    1:string code,
    2:string state,
	3:list<TRtValue> data,
}

service ThriftService{

    string HelloString(1:string para)
	map<string,TRtSnapshotData> readLatestData(1:list<string> codes)
	map<string, TRtHistoryData> readHistoryData(1:list<string> codes, 2:i32 startTime, 3:i32 endTime, 4:i32 interval)

}