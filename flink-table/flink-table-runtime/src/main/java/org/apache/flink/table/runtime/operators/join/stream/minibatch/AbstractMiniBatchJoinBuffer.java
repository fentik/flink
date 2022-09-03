package org.apache.flink.table.runtime.operators.join.stream.minibatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractMiniBatchJoinBuffer {
    private int currentBatchSize;
    private int currentEmittedCount;
    private int maxBatchSize;

    protected AbstractMiniBatchJoinBuffer(
            int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.currentBatchSize = 0;
        this.currentEmittedCount = 0;
    }

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractMiniBatchJoinBuffer.class);

    protected void recordAdded() {
        currentBatchSize += 1;
    }

    protected void recordEmitted() {
        currentEmittedCount += 1;
    }

    public boolean batchNeedsFlush() {
        return currentBatchSize > maxBatchSize;
    }

    protected void batchProcessed() {
        if (currentBatchSize > 100) {
            LOG.info("MINIBATCH emitted {} records out of {} recieved",
                    currentEmittedCount, currentBatchSize);
        }
        currentBatchSize = 0;
        currentEmittedCount = 0;
    }
}
