package cn.oge.kdm.rtdb.vzdb;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsConfig;
import cn.oge.kdm.rtdb.config.RtdbThreadsProperties;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbConnectionPool;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointReadAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointWriteAdapter;

@Configuration
public class VzdbConfig {

	@Bean
	public VzdbConnectionPool vzdbConnection(RtdbConfig config) {
		return new VzdbConnectionPool(config);
	}

	@Bean
	public VzdbPointReadAdapter readService(VzdbConnectionPool pool) {
		return new VzdbPointReadAdapter(pool);
	}

	@Bean
	public VzdbPointWriteAdapter writeService(VzdbConnectionPool pool) {
		return new VzdbPointWriteAdapter(pool);
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