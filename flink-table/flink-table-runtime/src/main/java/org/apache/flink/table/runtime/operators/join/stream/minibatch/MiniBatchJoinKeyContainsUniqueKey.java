package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinBatchProcessor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

public class MiniBatchJoinKeyContainsUniqueKey extends AbstractMiniBatchJoinBuffer
        implements MiniBatchJoinBuffer {
    private final String stateName;
    private final ValueState<RowData> bufferState;
    private final ValueStateDescriptor<RowData> bufferStateDesc;

    public MiniBatchJoinKeyContainsUniqueKey(
            RuntimeContext ctx,
            String stateName,
            InternalTypeInfo<RowData> recordType,
            int maxBatchSize) {
        super(maxBatchSize);
        this.stateName = stateName;
        this.bufferStateDesc = new ValueStateDescriptor<>(this.stateName, recordType);
        this.bufferState = ctx.getState(bufferStateDesc);
    }

    public void addRecordToBatch(RowData record) throws Exception {
        RowData prevRow = bufferState.value();
        if (prevRow == null) {
            // new state for key, could be acc or retract
            bufferState.update(record);
        } else {
            if (RowDataUtil.isAccumulateMsg(prevRow)) {
                if (RowDataUtil.isAccumulateMsg(record)) {
                    LOG.warn("MINIBATCH {} two sequential inserts!", this.getClass().getSimpleName());
                } else {
                    // existing message is +I/+U and incoming is -D/-U: clear state
                    bufferState.clear();
                }
            } else {
                if (RowDataUtil.isAccumulateMsg(record)) {
                    // existing message is -D/-U and incoming is +I/+U: clear
                    bufferState.clear();
                } else {
                    LOG.warn("MINIBATCH {} two sequential retractions!", this.getClass().getSimpleName());
                }
            }
        }

        recordAdded();
    }

    public void processBatch(KeyedStateBackend<RowData> be, JoinBatchProcessor processor) throws Exception {
        be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                bufferStateDesc,
                new KeyedStateFunction<RowData, ValueState<RowData>>() {
                    @Override
                    public void process(RowData key, ValueState<RowData> state) throws Exception {
                        int recordsEmitted = 0;
                        RowData record = state.value();
                        // set current key context for otherView fetch
                        be.setCurrentKey(key);
                        processor.process(record);
                        recordEmitted();
                        bufferState.clear();
                    }
                });
        batchProcessed();
    }

}
