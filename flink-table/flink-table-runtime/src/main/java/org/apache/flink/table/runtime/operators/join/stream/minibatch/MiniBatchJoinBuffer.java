package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import java.util.Map;
import java.util.HashMap;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinBatchProcessor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.runtime.util.RowDataStringSerializer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniBatchJoinBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(MiniBatchJoinBuffer.class);

    /*
     * On +I/+U increment
     * On -D/-U decrement
     * Then emit count retracts if negative or accumulate if positive
     */
    private final HashMap<RowData, Integer> buffer;
    private final KeySelector<RowData, RowData> keySelector;
    private final InternalTypeInfo<RowData> recordType;
    private final RowDataStringSerializer recordStringSerializer;

    private long currentBatchCount;
    private long currentEmittedCount;
    private long maxBatchCount;

    public MiniBatchJoinBuffer(
            InternalTypeInfo<RowData> recordType,
            KeySelector<RowData, RowData> keySelector,
            int maxBatchCount) {
        this.maxBatchCount = maxBatchCount;
        this.currentBatchCount = 0;
        this.currentEmittedCount = 0;
        this.buffer = new HashMap<>();
        this.keySelector = keySelector;
        this.recordType = recordType;
        this.recordStringSerializer = new RowDataStringSerializer(recordType);

    }

    public void addRecordToBatch(RowData input) throws Exception {
        BinaryRowData record = (BinaryRowData) input;
        int delta = RowDataUtil.isAccumulateMsg(record) ? 1 : -1;
        RowKind origKind = record.getRowKind();
        record.setRowKind(RowKind.INSERT);
        Integer cnt = buffer.get(record);

        LOG.info("MINIBATCH adding record {} cnt {}", recordAsString(input), cnt);

        if (cnt != null) {
            cnt += delta;
        } else {
            cnt = delta;
        }
        LOG.info("MINIBATCH no unique: cnt {} origkind {}", cnt, origKind);
        if (cnt == 0) {
            buffer.remove(record);
        } else {
            buffer.put(record.copy(), cnt);
        }

        recordAdded(record.getSizeInBytes());
    }

    public void processBatch(KeyedStateBackend<RowData> be, JoinBatchProcessor processor) throws Exception {
        for (Map.Entry<RowData, Integer> entry : buffer.entrySet()) {
            RowData record = entry.getKey();
            Integer count = entry.getValue();
            RowKind kind = count < 0 ? RowKind.DELETE : RowKind.INSERT;
            LOG.debug("MINIBATCH emitter rec {} kind {} count {}", recordAsString(record), kind, count);
            for (count = Math.abs(count); count > 0; count--) {
                // processor may overwrite kind, so reset it after every call
                be.setCurrentKey(keySelector.getKey(record));
                record.setRowKind(kind);
                processor.process(record);
                recordEmitted();
            }
        }
        buffer.clear();
        batchProcessed();
    }

    private void recordAdded(long byteSize) {
        currentBatchCount += 1;
    }

    private void recordEmitted() {
        currentEmittedCount += 1;
    }

    public boolean batchNeedsFlush() {
        return currentBatchCount > maxBatchCount;
    }

    private String recordAsString(RowData record) {
        return recordStringSerializer.asString(record);
    }

    private void batchProcessed() {
        if (currentBatchCount > 10) {
            LOG.info("MINIBATCH emitted {} records out of {} recieved",
                    currentEmittedCount, currentBatchCount);
        }
        currentBatchCount = 0;
        currentEmittedCount = 0;
    }
}
