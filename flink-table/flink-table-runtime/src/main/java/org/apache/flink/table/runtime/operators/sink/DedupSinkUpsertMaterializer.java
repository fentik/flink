/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

/**
 * It's not 100% obvious what the various errors the original SinkUpsertMaterializer
 * is meant to protect. However, for our purposes, it's an extremely expensive operator
 * as coded (it maintains a complete copy to the output dataset), and also runs into
 * some serious performance issues as coded.
 * 
 * On the performance side, the most glaring issue is the following: we see a number of
 * large (10k+) duplicate records come in. These duplicate records fill up a memtable
 * in our RocksDB backend very quickly. And the behavior we see is that the Sink gets into
 * a state where it's continously flushing memtables resulting in a terrible throughput.
 * 
 * In our copy of the Sink materializer, we are going to handle the following case which
 * we see with aggregates and rank function:
 *   +U ['Health', 'Granite Used Pizza', '1969.65']
 *   +U ['Health', 'Granite Used Pizza', '1969.65']
 *   -U ['Health', 'Granite Used Pizza', '1969.65']
 *   -U ['Health', 'Granite Used Pizza', '1969.65']
 * 
 * As you can see in the history, we get multiple "out of order" records for the same
 * key. More specifically, instead of +U/-U pairs, we see mutliple updates followed by
 * multiple retractions. If we were to simply forward such cases to the final sink
 * destination, we would produce an incorrect result. Imagine the following history:
 * 
 * For example, consider the following history:
 *   +U ['Health', 'Granite Used Pizza', '1969.65']
 *   +U ['Health', 'Granite Used Pizza', '1969.65']
 *   -U ['Health', 'Granite Used Pizza', '1969.65']
 * 
 * What second update precedes the retraction for the first row. In such a case, we would
 * erronesouly delete the record from destination because the delete is the last message
 * the we would process. The original SinkMaterializer handled this case by keeping a history
 * of all row versions for a key (resulting in a huge state and the performance issue desibed
 * above).
 * 
 * The reordering issue is transient. In our implementation, instead of persisting the complete
 * sink state in our durable backend, we will instead use an in-memory buffer to cache duplicate
 * records like this. The code assumes that such reordering cannot occur between mini-batch
 * watermarks, so we will use the mini-batch barrier imlementation to flush our state.
 *
 */

public class DedupSinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DedupSinkUpsertMaterializer.class);

    private final GeneratedRecordEqualiser generatedEqualiser;
    private final RowDataStringSerializer rowStringSerializer;
    private boolean isStreaming;

    private final int BUFFER_MAX_AGE_SEC = 60;
    private final int BUFFER_PRUNE_INTERVAL_SEC = 30;
    private transient long lastBufferPruneTime;

    private final LinkedHashMap<RowData, List<Tuple2<RowData, Long>>> buffer;
    private transient RecordEqualiser equaliser;
    private final KeySelector<RowData, RowData> keySelector;

    private transient TimestampedCollector<RowData> collector;

    public DedupSinkUpsertMaterializer(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> recordType,
            GeneratedRecordEqualiser generatedEqualiser,
            RowDataKeySelector keySelector,
            boolean isBatchBackfillEnabled) {
        this.generatedEqualiser = generatedEqualiser;
        this.rowStringSerializer = new RowDataStringSerializer(recordType.toRowType());
        this.keySelector = keySelector;
        this.isStreaming = isBatchBackfillEnabled ? false : true;
        this.buffer = new LinkedHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.collector = new TimestampedCollector<>(output);
        this.lastBufferPruneTime = System.currentTimeMillis();
    }

    private void emitLastRowForKey(List<Tuple2<RowData, Long>> values) {
        // emit the last accumulated message per unique key in a batch
        RowData lastRow = values.get(values.size() - 1).f0;
        collector.collect(lastRow);
    }

    private void flushBatch() throws Exception {
        for (Map.Entry<RowData, List<Tuple2<RowData, Long>>> entry : buffer.entrySet()) {
            emitLastRowForKey(entry.getValue());
        }

        buffer.clear();
        lastBufferPruneTime = System.currentTimeMillis();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (isStreaming) {
            flushBatch();
        }
        super.processWatermark(mark);
    }

    private void maybePruneBuffer() {
        long currentTime = System.currentTimeMillis();

        if (lastBufferPruneTime + BUFFER_PRUNE_INTERVAL_SEC * 1000 > currentTime) {
            return;
        }

        Iterator<List<Tuple2<RowData, Long>>> iter = buffer.values().iterator();
        while (iter.hasNext()) {
            List<Tuple2<RowData, Long>> values = iter.next();
            Long lastInsert = values.get(values.size() - 1).f1;
            if (lastInsert + BUFFER_MAX_AGE_SEC * 1000 < currentTime) {
                emitLastRowForKey(values);
                iter.remove();
            }
        }

        lastBufferPruneTime = currentTime;
    }

    @Override
    public void processElement(StreamRecord<RowData> input) throws Exception {
        BinaryRowData row = (BinaryRowData) input.getValue();
        RowKind origRowKind = row.getRowKind();

        if (this.shouldLogInput()) {
            LOG.info("{}: input {} mode {}",
                    getOperatorName(),
                    rowStringSerializer.asString(row),
                    isStreaming ? "STREAMING" : "BATCH BACKFILL");
        }

        if (!isStreaming) {
            // while not in streaming mode, we cannot experience the reordering
            // issue this operator is meant to address, so simply emit the input
            // row to the output
            collector.collect(row);
            return;
        }

        // copy out row for buffering since its reused
        row = row.copy();

        RowData key = keySelector.getKey(row);
        List<Tuple2<RowData, Long>> values = buffer.get(key);

        if (values == null) {
            if (!RowDataUtil.isAccumulateMsg(row)) {
                // we have no accumulated state for this retraction, so we just
                // pass it through as-is without buffering since we do not
                // expect to see retractions preceding accumulations for the
                // edge case this operation is meant to address
                collector.collect(row);
                return;
            } else {
                values = new ArrayList<Tuple2<RowData, Long>>();
                buffer.put(key, values);
            }
        }

        if (RowDataUtil.isAccumulateMsg(row)) {
            // always add accumulated messages to the buffer
            values.add(new Tuple2<>(row, System.currentTimeMillis()));
        } else {
            // retraction, find and remove the matching row
            Iterator<Tuple2<RowData, Long>> iter = values.iterator();
            boolean removed = false;
            while (iter.hasNext()) {
                RowData elementRow = iter.next().f0;
                // we need to make sure that row kind matches for comparison
                row.setRowKind(elementRow.getRowKind());
                if (equaliser.equals(elementRow, row)) {
                    iter.remove();
                    removed = true;
                    break;
                }
            }

            if (!removed) {
                row.setRowKind(origRowKind);
            }

            if (values.size() == 0) {
                // clear the buffer for this key if there are no elements
                // to keep the memory footprint down
                buffer.remove(key);
            }
        }

        maybePruneBuffer();
    }
}
