package cn.oge.kdm.rtdb.vzdb.adapter;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.profiler.ThreadNestedProfiler;

import cn.oge.kdm.common.utils.format.DateUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.data.domain.data.RtTimesData;
import cn.oge.kdm.data.domain.data.RtValueData;
import cn.oge.kdm.rtdb.adapter.RtdbBlockReadAdapter;
import cn.oge.kdm.rtdb.adapter.RtdbReadAdapter;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import vzdb.BlobData;
import vzdb.BlobDataListHolder;
import vzdb.Hdb;
import vzdb.RecordInfo;
import vzdb.RecordInfoSeqListHolder;
import vzdb.TagTm;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class VzdbBlockReadAdapter implements RtdbBlockReadAdapter {

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
			BlobDataListHolder holder = new BlobDataListHolder();

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.readBlobRealDatasByTags");
				back = hdb.readBlobRealDatasByTags(tags, holder);
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
				BlobData data = holder.value[i];
				if (data.tm == 0) { // 无数据
					result.put(tags[i], new RtSnapshotData());
				} else {
					result.put(tags[i], new RtSnapshotData(VzdbUtils.t2kdm(data.tm), data.val));
				}
			}
			return result;
		} finally {
			tnp.stop().log();
		}
	}

	@Override
	public Map<String, RtTimesData> readHistoryTimes(Collection<String> codes, Date startTime, Date endTime) {
		ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readHistoryTimes, log);
		try {
			// VZDB Param
			tnp.start("转换参数");
			String[] tags = codes.toArray(new String[codes.size()]);
			int start = DateUtils.toEpochSecond(startTime);
			int end = DateUtils.toEpochSecond(endTime);

			// VZDB Result
			int back = VzdbUtils.DEFAULT_VZDB_CODE;
			RecordInfoSeqListHolder holder = new RecordInfoSeqListHolder();

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.readBlobHisInfosByTags");
				back = hdb.readBlobHisInfosByTags(tags, start, end, holder);
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

			tnp.start("VZDB处理成功, 转换结果");
			Map<String, RtTimesData> result = new HashMap<>(codes.size());
			for (int i = 0; i < tags.length; i++) {
				RecordInfo[] data = holder.value[i];
				long[] times = new long[data == null ? 0 : data.length];
				for (int j = 0; j < times.length; j++) {
					times[j] = VzdbUtils.t2kdm(data[j].tm);
				}
				result.put(tags[i], new RtTimesData(times));
			}
			return result;
		} finally {
			tnp.stop().log();
		}
	}

	@Override
	public Map<String, RtValueData> readDataByTime(Collection<String> codes, Date time) {
		ThreadNestedProfiler tnp = ThreadNestedProfiler.find(RtdbReadAdapter.TNP_readDataByTime, log);
		try {
			// VZDB Param
			tnp.start("转换参数");
			TagTm[] tt = new TagTm[codes.size()];
			{
				int tm = DateUtils.toEpochSecond(time);
				int i = 0;
				for (String code : codes) {
					tt[i++] = new TagTm(code, tm);
				}
			}

			// VZDB Result
			int back = VzdbUtils.DEFAULT_VZDB_CODE;
			BlobDataListHolder holder = new BlobDataListHolder();

			Hdb hdb = null;
			try {
				tnp.start("获取VZDB连接");
				hdb = pool.acquire();
				tnp.start("调用VZDB.BlobDataListHolder");
				back = hdb.readBlobHisDataByTag(tt, holder);
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

			tnp.start("VZDB处理成功, 转换结果");
			Map<String, RtValueData> result = new HashMap<>(codes.size());
			for (int i = 0; i < tt.length; i++) {
				BlobData data = holder.value[i];
				if (data.tm == 0) { // 无数据
					result.put(tt[i].tag, null);
				} else {
					result.put(tt[i].tag, new RtValueData(data.val));
				}
			}
			return result;
		} finally {
			tnp.stop().log();
		}
	}
}
