package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinBatchProcessor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

public class MiniBatchJoinInputSideHasUniqueKey extends AbstractMiniBatchJoinBuffer
        implements MiniBatchJoinBuffer {
    private final String stateName;
    private final MapState<RowData, RowData> bufferState;
    private final MapStateDescriptor<RowData, RowData> bufferStateDesc;
    private final InternalTypeInfo<RowData> uniqueKeyType;
    private final KeySelector<RowData, RowData> uniqueKeySelector;

    public MiniBatchJoinInputSideHasUniqueKey(
            RuntimeContext ctx,
            String stateName,
            InternalTypeInfo<RowData> recordType,
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector,
            int maxBatchSize) {
        super(maxBatchSize);
        this.stateName = stateName;
        this.bufferStateDesc = new MapStateDescriptor<>(this.stateName, uniqueKeyType, recordType);
        this.bufferState = ctx.getMapState(bufferStateDesc);
        this.uniqueKeyType = uniqueKeyType;
        this.uniqueKeySelector = uniqueKeySelector;
    }

    @Override
    public void addRecordToBatch(RowData record) throws Exception {
        RowData uniqueKey = uniqueKeySelector.getKey(record);
        RowData prevRow = bufferState.get(uniqueKey);
        if (prevRow == null) {
            bufferState.put(uniqueKey, record);
        } else {
            if (RowDataUtil.isAccumulateMsg(record)) {
                if (RowDataUtil.isAccumulateMsg(prevRow)) {
                    LOG.warn("MINIBATCH invalid buffer state -- two sequential row additions!");
                } else {
                    bufferState.remove(uniqueKey);
                }
            } else {
                if (RowDataUtil.isAccumulateMsg(prevRow)) {
                    bufferState.put(uniqueKey, record);
                } else {
                    LOG.warn("MINIBATCH invalid buffer state -- two sequential row retractions!");
                }
            }
        }

        recordAdded();
    }

    @Override
    public void processBatch(KeyedStateBackend<RowData> be, JoinBatchProcessor processor) throws Exception {
        be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                bufferStateDesc,
                new KeyedStateFunction<RowData, MapState<RowData, RowData>>() {
                    @Override
                    public void process(RowData key, MapState<RowData, RowData> state) throws Exception {
                        be.setCurrentKey(key);
                        for (RowData record : state.values()) {
                            processor.process(record);
                            recordEmitted();
                        }
                        bufferState.clear();
                    }
                });
        batchProcessed();
    }

}
