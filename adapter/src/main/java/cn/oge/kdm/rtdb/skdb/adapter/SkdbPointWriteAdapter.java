package cn.oge.kdm.rtdb.skdb.adapter;

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
public class SkdbPointWriteAdapter implements RtdbWriteAdapter {
    @Setter
    private SkdbConnectionPool pool;
    @Override
    public Map<String, RtMetaData> writeData(Collection<RtHistoryData> data) {
        return null;
    }
}
