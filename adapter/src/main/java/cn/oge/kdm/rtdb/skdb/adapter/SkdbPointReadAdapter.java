package cn.oge.kdm.rtdb.skdb.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Setter;
import org.slf4j.profiler.ThreadNestedProfiler;

import cn.com.skdb.api.ISkApi;
import cn.com.skdb.api.SkFactory;
import cn.com.skdb.api.SkHisval;
import cn.com.skdb.api.SkNowval;
import cn.com.skdb.api.SkValue;
import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.data.domain.value.RtValue;
import cn.oge.kdm.rtdb.adapter.RtdbPointReadAdapter;
import cn.oge.kdm.rtdb.adapter.RtdbReadAdapter;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbUtils;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class SkdbPointReadAdapter implements RtdbPointReadAdapter {
	
    @Setter
    private SkdbConnectionPool pool;

    @Override
    public Map<String, RtSnapshotData> readLatestData(Collection<String> codes) {
        ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readLatestData, log);
        try {

            // VZDB Param
            tnp.start("转换参数");
            List<String> tags = new ArrayList<>(codes);
            System.out.println(tags);
            //
            List<SkNowval> skNowvals = null;
            ISkApi connect = null;
            try {
                tnp.start("获取Skdb连接");
                connect = pool.acquire();
                tnp.start("调用Skdb.GetNowValue");
                skNowvals = connect.GetNowValue(tags);
            } catch (Exception e) {
            	throw new RtdbAdapterException(RtDataState.DS_CONNECT_FAIL);
            } finally {
                if (connect != null) {
                	tnp.start("释放Skdb连接");
                	pool.release(connect);
				}
            }
            tnp.start("Skdb处理完成, 转换结果");
            Map<String, RtSnapshotData> result = new HashMap<>(codes.size());
            for (int i = 0; i < tags.size(); i++) {
                SkNowval skNowval = skNowvals.get(i);
                if (skNowval.value.Status == 0) { // 无别名
                    result.put(tags.get(i), new RtSnapshotData(RtDataState.DS_NO_ALIAS));
                } else {
                    int time = (int) (skNowval.value.Time.getTime() / 1000);
                    float value = Float.valueOf(skNowval.value.Value.toString());
                    result.put(tags.get(i), new RtSnapshotData(VzdbUtils.tv2kdm(time, value)));
                }
            }
            return result;
        } finally {
            tnp.stop().log();
        }
    }

    @Override
    public Map<String, RtHistoryData> readHistoryData(Collection<String> codes, Date startTime, Date endTime, int interval) {
        ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readLatestData, log);
        try {

            // Skdb get tags list
            tnp.start("转换参数");
            List<String> tags = new ArrayList<>(codes);
            //Skdb history values list 
            List<SkHisval> skHisvals = new ArrayList<>();
            ISkApi connect = null;
            try {
                tnp.start("获取Skdb连接");
                connect = pool.acquire();
                tnp.start("调用Skdb.GetHistoryValue");
                if (interval > 0) {
                	skHisvals = connect.GetHistoryValue(tags, startTime, endTime,interval);
				}else {
					skHisvals = connect.GetHistoryValue(tags, startTime, endTime);
				}
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
//                tnp.start("释放Skdb连接");
////              connect.Close();
            }
            tnp.start("Skdb处理完成, 转换结果");
            Map<String, RtHistoryData> result = new HashMap<>(codes.size());
            for (SkHisval skHisval : skHisvals) {
                List<SkValue> skValues = skHisval.values;
                String tag = skHisval.Cpid;
                RtHistoryData rtHistoryData = new RtHistoryData();
                for (SkValue skValue : skValues) {
                    int time = (int) (skValue.Time.getTime() / 1000);
                    float value = Float.valueOf(skValue.Value.toString());
                    RtValue rtValue = VzdbUtils.tv2kdm(time, value);
                    rtHistoryData.addData(rtValue);
                }
                result.put(tag,rtHistoryData);
            }
            return result;
        } finally {
            tnp.stop().log();
        }
    }
    
    

}
