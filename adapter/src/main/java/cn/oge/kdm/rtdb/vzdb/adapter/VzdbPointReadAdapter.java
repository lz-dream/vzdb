package cn.oge.kdm.rtdb.vzdb.adapter;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.profiler.ThreadNestedProfiler;

import com.google.common.base.Function;

import cn.oge.kdm.common.utils.format.DateUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtMetaData;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.data.domain.data.RtTimesData;
import cn.oge.kdm.data.domain.data.RtValueData;
import cn.oge.kdm.rtdb.adapter.RtdbPointReadAdapter;
import cn.oge.kdm.rtdb.adapter.RtdbReadAdapter;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import vzdb.FloatHisData;
import vzdb.FloatRealData;
import vzdb.FloatRealDataListHolder;
import vzdb.FloatSectionTableData;
import vzdb.FloatSectionTableDataListHolder;
import vzdb.Hdb;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class VzdbPointReadAdapter implements RtdbPointReadAdapter {

	// 某些情况下（如使用步长查询没有数据的时间段）VZDB会返回一个及其大的无效数据
	// 通过此处的设定进行过滤
	private static final float MAX_POINT_VAL = 100_000_000_000_000F;

	@Setter
	private VzdbConnectionPool pool;

	@Override
	public Map<String, RtSnapshotData> readLatestData(Collection<String> codes) {
		ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readLatestData, log);
		try {

			// VZDB Param
			tnp.start("转换参数");
			String[] tags = codes.toArray(new String[codes.size()]);

			// VZDB Result
			int back = VzdbUtils.DEFAULT_VZDB_CODE;
			FloatRealDataListHolder holder = new FloatRealDataListHolder();

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.readFloatRealDatasByTags");
				back = hdb.readFloatRealDatasByTags(tags, holder);
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

			tnp.start("VZDB处理失败, 转换结果");
			Map<String, RtSnapshotData> result = new HashMap<>(codes.size());
			for (int i = 0; i < tags.length; i++) {
				FloatRealData data = holder.value[i];
				if (data.id == 0) { // 无别名
					result.put(tags[i], new RtSnapshotData(RtDataState.DS_NO_ALIAS));
				} else if (data.tm == 0) { // 无数据
					result.put(tags[i], new RtSnapshotData());
				} else {
					result.put(tags[i], new RtSnapshotData(VzdbUtils.tv2kdm(data.tm, data.val)));
				}
			}
			return result;
		} finally {
			tnp.stop().log();
		}
	}

	private <T extends RtMetaData> Map<String, T> read(String name, Collection<String> codes, Date startTime, Date endTime, int interval,
			Supplier<T> maker, Function<FloatSectionTableData, T> converter) {
		ThreadNestedProfiler tnp = ThreadNestedProfiler.find(name, log);
		try {

			// VZDB Param
			tnp.start("转换参数");
			String[] tags = codes.toArray(new String[codes.size()]);
			int start = DateUtils.toEpochSecond(startTime);
			int end = DateUtils.toEpochSecond(endTime);

			// VZDB Result
			int back = VzdbUtils.DEFAULT_VZDB_CODE;
			FloatSectionTableDataListHolder holder = new FloatSectionTableDataListHolder();

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.readFloatSectionTableDatasByTags");
				back = hdb.readFloatSectionTableDatasByTags(tags, start, end, interval, vzdb.InterpolationLine.value, holder);
			} finally {
				tnp.start("释放VZDB连接");
				pool.release(hdb);
			}

			if (back < 0) {
				tnp.start("VZDB处理失败, 返回代码:" + back);
				// 为-2表示连接中断
				if (back == -2) throw new RtdbAdapterException(RtDataState.DS_CONNECT_INTERRUPT);
				// 其他为处理失败
				throw new RtdbAdapterException(RtDataState.DS_PROCESS_FAIL);
			}

			tnp.start("VZDB处理失败, 转换结果");
			Map<String, T> result = new HashMap<>(codes.size());
			for (int i = 0; i < tags.length; i++) {
				FloatSectionTableData data = holder.value[i];
				if (data.id == 0) { // 无别名
					T t = maker.get();
					t.setState(RtDataState.DS_NO_ALIAS);
					result.put(tags[i], t);
				} else {
					T rhd = converter.apply(data);
					result.put(tags[i], rhd);
				}
			}
			return result;
		} finally {
			tnp.stop().log();
		}
	}

	@Override
	public Map<String, RtHistoryData> readHistoryData(Collection<String> codes, Date startTime, Date endTime, int interval) {
		return read(RtdbReadAdapter.TNP_readHistoryData, codes, startTime, endTime, interval, () -> new RtHistoryData(), (data) -> {
			RtHistoryData rhd = new RtHistoryData();
			for (FloatHisData d : data.dataList) {
				if (d.val <= MAX_POINT_VAL) rhd.addData(VzdbUtils.tv2kdm(d.tm, d.val));
			}
			return rhd;
		});
	}

	@Override
	public Map<String, RtTimesData> readHistoryTimes(Collection<String> codes, Date startTime, Date endTime) {
		return read(RtdbReadAdapter.TNP_readHistoryTimes, codes, startTime, endTime, 0, () -> new RtTimesData(), (data) -> {
			long[] times = new long[data == null || data.dataList == null ? 0 : data.dataList.length];
			for (int i = 0; i < times.length; i++) {
				times[i] = VzdbUtils.t2kdm(data.dataList[i].tm);
			}
			return new RtTimesData(times);
		});
	}

	@Override
	public Map<String, RtValueData> readDataByTime(Collection<String> codes, Date time) {
		return read(RtdbReadAdapter.TNP_readDataByTime, codes, time, time, 0, () -> new RtValueData(), (data) -> new RtValueData(
				data == null || ArrayUtils.isEmpty(data.dataList) ? null : VzdbUtils.v2kdm(data.dataList[0].val)));
	}

}
