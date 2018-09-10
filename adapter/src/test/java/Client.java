import cn.oge.kdm.rtdb.thrift.source.TRtHistoryData;
import cn.oge.kdm.rtdb.thrift.source.TRtSnapshotData;
import cn.oge.kdm.rtdb.thrift.source.ThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Client {
    public static void main(String[] args) {
        ThriftService.Client connect = null;
        TTransport transport = null;
        try {
            transport = new TSocket("127.0.0.1", 9090);
            transport.open();
            TProtocol protocol = new TCompactProtocol(transport);
            connect = new ThriftService.Client(protocol);

            List<String> codes = new ArrayList<>();
            codes.add("OP.1DCS.01BHT03AI01_XQ01");
            long start1 = new Date().getTime();
            System.out.println("开始： "+ start1);
            Map<String, TRtHistoryData> readHistoryData = connect.readHistoryData(codes, 1535817600, 1535904000, 6);
            long end1 = new Date().getTime();
            System.out.println("结束： "+end1+"   获取数据用时，"+(end1-start1));
        }catch (TException x) {
            x.printStackTrace();
        }
        finally {
            transport.close();
        }
    }
}
