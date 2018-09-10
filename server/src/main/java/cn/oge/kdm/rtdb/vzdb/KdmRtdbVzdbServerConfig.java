package cn.oge.kdm.rtdb.vzdb;

import cn.oge.kdm.rtdb.skdb.adapter.SkdbConnectionPool;
import cn.oge.kdm.rtdb.skdb.adapter.SkdbPointReadAdapter;
import cn.oge.kdm.rtdb.skdb.adapter.SkdbPointWriteAdapter;
import cn.oge.kdm.rtdb.thrift.adapter.ThriftConnectionPool;
import cn.oge.kdm.rtdb.thrift.adapter.ThriftPointReadAdapter;
import cn.oge.kdm.rtdb.thrift.adapter.ThriftPointWriteAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbConnectionPool;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointReadAdapter;
import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointWriteAdapter;
import org.springframework.context.annotation.Scope;

@Configuration
public class KdmRtdbVzdbServerConfig {
//
//	@Bean
//	public VzdbConnectionPool vzdbConnection(RtdbConfig config) {
//		return new VzdbConnectionPool(config);
//	}
//
//	@Bean
//	public VzdbPointReadAdapter readService(VzdbConnectionPool pool) {
//		return new VzdbPointReadAdapter(pool);
//	}
//
//	@Bean
//	public VzdbPointWriteAdapter writeService(VzdbConnectionPool pool) {
//		return new VzdbPointWriteAdapter(pool);
//	}
//
//	//-----------------skdb-----------------
//    @Bean
//    public SkdbConnectionPool skdbConnection(RtdbConfig config) {
//        return new SkdbConnectionPool(config);
//    }
//
//    @Bean
//    public SkdbPointReadAdapter skdbReadService(SkdbConnectionPool pool) {
//        return new SkdbPointReadAdapter(pool);
//    }
//
//    @Bean
//    public SkdbPointWriteAdapter skdbWriteService(SkdbConnectionPool pool) {
//        return new SkdbPointWriteAdapter(pool);
//    }

    //-----------------thrift-----------------
    @Bean
    public ThriftConnectionPool thriftConnection(RtdbConfig config) {
        return new ThriftConnectionPool(config);
    }

    @Bean
    public ThriftPointReadAdapter thriftReadService(ThriftConnectionPool pool) {
        return new ThriftPointReadAdapter(pool);
    }

    @Bean
    public ThriftPointWriteAdapter thriftWriteService(ThriftConnectionPool pool) {
        return new ThriftPointWriteAdapter(pool);
    }
}