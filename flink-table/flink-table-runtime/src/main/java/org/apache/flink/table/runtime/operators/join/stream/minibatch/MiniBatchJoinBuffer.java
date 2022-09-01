package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinBatchProcessor;

public interface MiniBatchJoinBuffer {
    abstract public void processBatch(KeyedStateBackend<RowData> be, JoinBatchProcessor processor) throws Exception;

    abstract public void addRecordToBatch(RowData record) throws Exception;
}
