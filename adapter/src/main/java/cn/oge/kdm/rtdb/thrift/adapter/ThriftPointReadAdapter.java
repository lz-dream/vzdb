package cn.oge.kdm.rtdb.thrift.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.value.RtValue;
import cn.oge.kdm.rtdb.thrift.source.TRtValue;
import com.alibaba.fastjson.JSON;
import org.apache.thrift.TException;
import org.slf4j.profiler.ThreadNestedProfiler;


import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.rtdb.adapter.RtdbPointReadAdapter;
import cn.oge.kdm.rtdb.adapter.RtdbReadAdapter;
import cn.oge.kdm.rtdb.thrift.source.ThriftService.Client;
import cn.oge.kdm.rtdb.thrift.source.TRtHistoryData;
import cn.oge.kdm.rtdb.thrift.source.TRtSnapshotData;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class ThriftPointReadAdapter implements RtdbPointReadAdapter {

    @Setter
    private ThriftConnectionPool pool;

    @Override
    public Map<String, RtSnapshotData> readLatestData(Collection<String> codes) {
        ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readLatestData, log);
        try {
            tnp.start("参数转换");
            List<String> tags = new ArrayList<>(codes);
            System.out.println(tags);
            tnp.start("获取thrift连接");
            Client client = pool.acquire();
            Map<String, RtSnapshotData> valueMap = new HashMap<>(codes.size());
            if (client != null) {
                tnp.start("查询数据");
                Map<String, TRtSnapshotData> readLatestData = client.readLatestData(tags);

                tnp.start("处理数据");


                for (Entry<String, TRtSnapshotData> entry : readLatestData.entrySet()) {
                    TRtSnapshotData tRtSnapshotData = entry.getValue();
                    RtSnapshotData rtSnapshotData = JSON.parseObject(JSON.toJSONString(tRtSnapshotData), RtSnapshotData.class);
                    valueMap.put(entry.getKey(), rtSnapshotData);
                }
            }
            return valueMap;

        } catch (TException e) {
            pool.connect = null;
            e.printStackTrace();
            return null;
        } finally {
            tnp.stop().log();
        }

    }

    @Override
    public Map<String, RtHistoryData> readHistoryData(Collection<String> codes, Date startTime, Date endTime,
                                                      int interval) {
        ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readLatestData, log);
        try {
            tnp.start("参数转换");
            List<String> tags = new ArrayList<>(codes);
            System.out.println(tags);
            int start_time = Integer.parseInt(Long.toString(startTime.getTime() / 1000));
            int end_time = Integer.parseInt(Long.toString(endTime.getTime() / 1000));

            tnp.start("获取thrift连接");
            Client client = pool.acquire();
            Map<String, RtHistoryData> valueMap = new HashMap<>();
            if (client != null) {
                tnp.start("获取数据");
                long start1 = new Date().getTime();
                Map<String, TRtHistoryData> readHistoryData = client.readHistoryData(tags, start_time, end_time, interval / 1000);
                long end1 = new Date().getTime();
                System.out.println("获取数据用时，"+(end1-start1));

                tnp.start("处理数据");
                long start2 = new Date().getTime();
//                for (Entry<String, TRtHistoryData> entry : readHistoryData.entrySet()) {
//                    TRtHistoryData tRtHistoryData = entry.getValue();
//                    RtHistoryData rtHistoryData = JSON.parseObject(JSON.toJSONString(tRtHistoryData), RtHistoryData.class);
//                    valueMap.put(entry.getKey(), rtHistoryData);
//                }
                for (Entry<String, TRtHistoryData> entry : readHistoryData.entrySet()) {
                    TRtHistoryData tRtHistoryData = entry.getValue();
                    RtHistoryData rtHistoryData = new RtHistoryData();
                    rtHistoryData.setCode(tRtHistoryData.code);
                    if (tRtHistoryData.state != null ) {
                        rtHistoryData.setState(RtDataState.valueOf(tRtHistoryData.state));
                    }
                    if (tRtHistoryData.data != null){
                        List<TRtValue> tRtValues = tRtHistoryData.data;
                        List<RtValue> rtValues = new ArrayList<>();
                        for (TRtValue tRtValue : tRtValues) {
                            RtValue rtValue = new RtValue();
                            rtValue.setTime(tRtValue.time);
                            rtValue.setValue(tRtValue.value);
                            rtValues.add(rtValue);
                        }
                        rtHistoryData.setData(rtValues);
                    }
                    valueMap.put(entry.getKey(), rtHistoryData);
                }
                long end2 = new Date().getTime();
                System.out.println("处理数据用时，"+(end2-start2));
            }
            pool.release(client);

            return valueMap;
        } catch (TException e) {
            pool.connect = null;
            e.printStackTrace();
            return null;
        } finally {
            tnp.stop().log();
        }

    }


}
