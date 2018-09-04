package cn.oge.kdm.rtdb.vzdb;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsProperties;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbBlockReadAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbBlockWriteAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbConnectionPool;

@Configuration
public class KdmRtdbVzdbBlockServerConfig {

	@Bean
	public VzdbConnectionPool vzdbConnection(RtdbConfig config) {
		return new VzdbConnectionPool(config);
	}

	@Bean
	public VzdbBlockReadAdapter readService(VzdbConnectionPool pool) {
		return new VzdbBlockReadAdapter(pool);
	}

	@Bean
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