package cn.oge.kdm.rtdb.vzdb.adapter;

import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import lombok.extern.slf4j.Slf4j;
import vzdb.Hdb;

@Slf4j
public class VzdbConnectionPool {

	private RtdbConfig config;

	public VzdbConnectionPool(RtdbConfig custom) {
		RtdbConfig defaults = new RtdbConfig();
		defaults.setHost("127.0.0.1");
		defaults.setPort(22235);
		defaults.setUsername("root");
		defaults.setPassword("root1234");
		BeanUtils.override(defaults, custom);
		this.config = defaults;
	}

	private Hdb rdb = null;

	public synchronized Hdb acquire() {
		if (rdb == null) {
			int rcode = -9999;
			try {
				rdb = new Hdb();
				System.out.println(config.getHost());
            	System.out.println(config.getPort());
            	System.out.println(config.getUsername());
            	System.out.println(config.getPassword());
				rcode = rdb.connect(config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
				log.info("vzdb连接成功");
			} catch (Throwable t) {
				rdb = null;
				log.error("VZDB连接异常", t);
				throw new RtdbAdapterException(RtDataState.DS_ERROR);
			}

			if (rcode != vzdb.ConnectSucceed.value) throw new RtdbAdapterException(RtDataState.DS_CONNECT_FAIL);
		}
		return rdb;
	}

	public void release(Hdb hdb) {
		// Do Nothing
	}
}
