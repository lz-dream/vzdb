package cn.oge.kdm.rtdb.vzdb;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsProperties;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbBlockReadAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbBlockWriteAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbConnectionPool;

public class VzdbBlockConfig {

	@Bean
	public VzdbConnectionPool vzdbConnection(RtdbConfig config) {
		return new VzdbConnectionPool(config);
	}

	@Bean
	@Primary
	public VzdbBlockReadAdapter readService(VzdbConnectionPool pool) {
		return new VzdbBlockReadAdapter(pool);
	}

	@Bean
	@Primary
	public VzdbBlockWriteAdapter writeService(VzdbConnectionPool pool) {
		return new VzdbBlockWriteAdapter(pool);
	}

	@Bean
	@Primary
	public RtdbThreadsConfig rtdbThreadsConfig(RtdbThreadsProperties custom) {
		RtdbThreadsConfig defaults = new RtdbThreadsConfig();
		defaults.setReadHistoryData(50);
		defaults.setReadHistoryTimes(50);
		BeanUtils.override(defaults, custom);
		return defaults;
	}

}