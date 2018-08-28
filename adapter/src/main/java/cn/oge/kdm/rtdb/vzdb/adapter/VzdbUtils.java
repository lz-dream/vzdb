package cn.oge.kdm.rtdb.vzdb.adapter;

import org.apache.commons.lang3.time.DateUtils;

import cn.oge.kdm.data.domain.value.RtValue;
import cn.oge.kdm.data.util.DataUtils;

public class VzdbUtils {

	public static final int DEFAULT_VZDB_CODE = -999;

	public static int t2db(long tm) {
		return (int) (tm / DateUtils.MILLIS_PER_SECOND);
	}

	public static long t2kdm(int tm) {
		return tm * DateUtils.MILLIS_PER_SECOND;
	}

	public static float v2kdm(float val) {
		return DataUtils.roundValue(val, 4);
	}

	public static RtValue tv2kdm(int time, float value) {
		RtValue result = new RtValue();
		result.setTime(t2kdm(time));
		result.setValue(v2kdm(value)); // 保留4位
		return result;
	}
}
