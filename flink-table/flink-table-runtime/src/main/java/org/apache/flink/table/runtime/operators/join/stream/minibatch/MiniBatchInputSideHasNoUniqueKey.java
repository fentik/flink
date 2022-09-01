package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinBatchProcessor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

public class MiniBatchInputSideHasNoUniqueKey extends AbstractMiniBatchJoinBuffer
        implements MiniBatchJoinBuffer {
    /*
     * On +I/+U increment
     * On -D/-U decrement
     * Then emit count retracts if negative or accumulate if positive
     */
    private final MapState<RowData, Integer> bufferState;
    private final MapStateDescriptor<RowData, Integer> bufferStateDesc;
    private final String stateName;

    public MiniBatchInputSideHasNoUniqueKey(
            RuntimeContext ctx,
            String stateName,
            InternalTypeInfo<RowData> recordType) {
        this.stateName = stateName;
        this.bufferStateDesc = new MapStateDescriptor<>(this.stateName, recordType, Types.INT);
        this.bufferState = ctx.getMapState(bufferStateDesc);
    }

    public void addRecordToBatch(RowData record) throws Exception {
        int delta = RowDataUtil.isAccumulateMsg(record) ? 1 : -1;
        RowKind origKind = record.getRowKind();
        record.setRowKind(RowKind.INSERT);
        Integer cnt = bufferState.get(record);
        LOG.info("MINIBATCH fetched count from state {}", cnt);

        if (cnt != null) {
            cnt += delta;
        } else {
            cnt = delta;
        }
        LOG.info("MINIBATCH no unique: cnt {} origkind {}", cnt, origKind);
        if (cnt == 0) {
            bufferState.clear();
        } else {
            bufferState.put(record, cnt);
        }
    }

    public void processBatch(KeyedStateBackend<RowData> be, JoinBatchProcessor processor) throws Exception {
        LOG.info("MINIBATCH emit for {}", stateName);

        be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                bufferStateDesc,
                new KeyedStateFunction<RowData, MapState<RowData, Integer>>() {
                    @Override
                    public void process(RowData key, MapState<RowData, Integer> state) throws Exception {
                        be.setCurrentKey(key);

                        for (Map.Entry<RowData, Integer> entry : state.entries()) {
                            RowData record = entry.getKey();
                            Integer count = entry.getValue();
                            RowKind kind = count < 0 ? RowKind.DELETE : RowKind.INSERT;
                            while (count > 0) {
                                // processor may overwrite kind, so reset it after every call
                                LOG.info("MINIBATCH emit record {} kind {}", record, kind);
                                record.setRowKind(kind);
                                processor.process(record);
                                count--;
                            }
                        }
                        bufferState.clear();
                    }
                });
    }
}
