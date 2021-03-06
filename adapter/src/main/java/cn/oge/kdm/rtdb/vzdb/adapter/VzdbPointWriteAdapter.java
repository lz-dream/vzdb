package cn.oge.kdm.rtdb.vzdb.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.profiler.ThreadNestedProfiler;

import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtMetaData;
import cn.oge.kdm.rtdb.adapter.RtdbWriteAdapter;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import vzdb.FloatTagData;
import vzdb.Hdb;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class VzdbPointWriteAdapter implements RtdbWriteAdapter {

	@Setter
	private VzdbConnectionPool pool;

	@Override
	public Map<String, RtMetaData> writeData(Collection<RtHistoryData> param) {
		ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbWriteAdapter.TNP_writeData, log);
		try {
			// VZDB Param
			tnp.start("转换参数");
			FloatTagData[] data = convert(param);

			// VZDB Result
			int back = VzdbUtils.DEFAULT_VZDB_CODE;

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.writeFloatHisDatasByTags");
				back = hdb.writeFloatHisDatasByTags(data);
			} finally {
				tnp.start("释放VZDB连接");
				pool.release(hdb);
			}

			if (back != 1) {
				tnp.start("VZDB处理失败, 返回代码:" + back);
				// 为-2表示连接失败
				if (back == -2) throw new RtdbAdapterException(RtDataState.DS_CONNECT_INTERRUPT);
				// 其他为处理失败
				throw new RtdbAdapterException(RtDataState.DS_PROCESS_FAIL);
			}
			return new HashMap<>();
		} finally {
			tnp.stop().log();
		}
	}

	private static FloatTagData[] convert(Collection<RtHistoryData> param) {
		List<FloatTagData> data = new ArrayList<>();
		param.forEach((dc) -> {
			dc.getData().forEach((d) -> {
				FloatTagData td = new FloatTagData();
				td.tm = VzdbUtils.t2db(d.getTime());
				td.tag = dc.getCode();
				td.val = d.getNumberValue().floatValue();
				td.flag = 1;
				data.add(td);
			});
		});
		return data.toArray(new FloatTagData[data.size()]);
	}

}
