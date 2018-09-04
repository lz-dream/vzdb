package cn.oge.kdm.rtdb.vzdb;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbConnectionPool;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointReadAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointWriteAdapter;

@Configuration
public class KdmRtdbVzdbServerConfig {

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
}