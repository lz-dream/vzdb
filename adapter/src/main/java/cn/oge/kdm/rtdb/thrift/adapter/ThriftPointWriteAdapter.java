package cn.oge.kdm.rtdb.thrift.adapter;

import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtMetaData;
import cn.oge.kdm.rtdb.adapter.RtdbWriteAdapter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class ThriftPointWriteAdapter implements RtdbWriteAdapter {
    @Setter
    private ThriftConnectionPool pool;
    @Override
    public Map<String, RtMetaData> writeData(Collection<RtHistoryData> data) {
        return null;
    }
}
