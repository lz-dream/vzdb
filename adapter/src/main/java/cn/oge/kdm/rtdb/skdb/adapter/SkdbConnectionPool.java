package cn.oge.kdm.rtdb.skdb.adapter;


import cn.com.skdb.api.ISkApi;
import cn.com.skdb.api.SkFactory;
import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SkdbConnectionPool {

    private RtdbConfig config;

    public SkdbConnectionPool(RtdbConfig custom) {
        RtdbConfig defaults = new RtdbConfig();
        defaults.setHost("127.0.0.1");
        defaults.setPort(22235);
        defaults.setUsername("root");
        defaults.setPassword("root1234");
        BeanUtils.override(defaults, custom);
        this.config = defaults;
    }
    private ISkApi connect = null;
    public  ISkApi acquire(){
        if (connect == null){
            try {
            	System.out.println(config.getHost());
            	System.out.println(config.getPort());
            	System.out.println(config.getUsername());
            	System.out.println(config.getPassword());
                connect= SkFactory.CreateApi(config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
                log.info("skdb连接成功");

            } catch (Exception e) {
                connect = null;
                throw new RtdbAdapterException(RtDataState.DS_CONNECT_FAIL);
            }
        }
        return connect;
    }
	public void release(ISkApi connect) {
//		connect.Close();
        //Bean为单实例模式，这里关闭会有问题
	}
}
