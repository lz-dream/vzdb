package cn.oge.kdm.rtdb.thrift.adapter;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cn.oge.kdm.common.utils.beans.BeanUtils;
import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.rtdb.adapter.exception.RtdbAdapterException;
import cn.oge.kdm.rtdb.config.RtdbConfig;
import cn.oge.kdm.rtdb.thrift.source.ThriftService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThriftConnectionPool {

    private RtdbConfig config;

    public ThriftConnectionPool(RtdbConfig custom) {
        RtdbConfig defaults = new RtdbConfig();
        defaults.setHost("127.0.0.1");
        defaults.setPort(22235);
        defaults.setUsername("root");
        defaults.setPassword("root1234");
        BeanUtils.override(defaults, custom);
        this.config = defaults;
    }

    ThriftService.Client connect = null;
    TTransport transport = null;

    public ThriftService.Client acquire() {
        if (connect == null){
            try {
                transport = new TSocket(config.getHost(), config.getPort());
                TProtocol protocol = new TCompactProtocol(transport);
                connect = new ThriftService.Client(protocol);
                transport.open();
                log.info("opapi连接成功");
            } catch (Exception e) {
                connect = null;
                throw new RtdbAdapterException(RtDataState.DS_CONNECT_FAIL);
            }
        }
        return connect;
    }

    public void release(ThriftService.Client connect) {
//        transport.close();
        //do nothing
    }
}