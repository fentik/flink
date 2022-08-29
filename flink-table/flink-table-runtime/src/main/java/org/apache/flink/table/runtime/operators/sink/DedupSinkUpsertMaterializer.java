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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.table.types.utils.TypeConversions;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

/**
 * An operator that maintains incoming records in state corresponding to the upsert keys and
 * generates an upsert view for the downstream operator.
 *
 * <ul>
 *   <li>Adds an insertion to state and emits it with updated {@link RowKind}.
 *   <li>Applies a deletion to state.
 *   <li>Emits a deletion with updated {@link RowKind} iff affects the last record or the state is
 *       empty afterwards. A deletion to an already updated record is swallowed.
 * </ul>
 */
public class DedupSinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DedupSinkUpsertMaterializer.class);

    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. This will result in incorrect result. "
                    + "You can increase the state ttl to avoid this.";

    private final StateTtlConfig ttlConfig;
    private final InternalTypeInfo<RowData> recordType;
    private final GeneratedRecordEqualiser generatedEqualiser;
    private final RowDataStringSerializer rowStringSerializer;
    private final boolean isBatchBackfillEnabled; 
    private int maxValueLength = 1;
    private boolean isStreaming;

    private transient RecordEqualiser equaliser;

    // Buffer of emitted insertions on which deletions will be applied first.
    // The row kind might be +I or +U and will be ignored when applying the deletion.
    private transient ValueState<List<Tuple2<RowData, Integer>>> state;
    private transient TimestampedCollector<RowData> collector;

    public DedupSinkUpsertMaterializer(
        StateTtlConfig ttlConfig,
        InternalTypeInfo<RowData> recordType,
        GeneratedRecordEqualiser generatedEqualiser,
        boolean isBatchBackfillEnabled) {
        this.ttlConfig = ttlConfig;
        this.recordType = recordType;
        this.generatedEqualiser = generatedEqualiser;
        this.rowStringSerializer = new RowDataStringSerializer(recordType.toRowType());
        this.isBatchBackfillEnabled = isBatchBackfillEnabled;
        this.isStreaming = isBatchBackfillEnabled ? false : true;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.equaliser =
                generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        ListTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo = new ListTypeInfo<>(new TupleTypeInfo(recordType, Types.INT));
        ValueStateDescriptor<List<Tuple2<RowData, Integer>>> descriptor =
                new ValueStateDescriptor<>("values", valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        this.state = getRuntimeContext().getState(descriptor);
        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // we don't do any batch processing in this operator, we're simply looking at the state
        // for diagnostic purposes
        if (!isStreaming) {
            if (mark.getTimestamp() == Watermark.MAX_WATERMARK.getTimestamp()) {
                isStreaming = true;
                maxValueLength = 1;
            }
        }
        super.processWatermark(mark);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData row = element.getValue();
        List<Tuple2<RowData, Integer>> values = state.value();
        if (values == null) {
            values = new ArrayList<>(2);
        }
        if (this.shouldLogInput()) {
            LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with "
                + values.size() + " values : " + rowStringSerializer.asString(row));
        }
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                 if (values.size() > maxValueLength) {
                    maxValueLength = values.size();
                    LOG.info("DEDUP new maxValueLength {} for row {} iStreaming {}", maxValueLength, rowStringSerializer.asString(row), isStreaming);
                 }
                row.setRowKind(values.isEmpty() ? INSERT : UPDATE_AFTER);
                // Add a new row if it doesn't exist, otherwise update the tuple with
                // a new count to remove duplicates
                boolean seen = false;
                int i = 0;
                for (Tuple2<RowData, Integer> entry : values) {
                    RowData currRow = entry.f0;
                    Integer count = entry.f1;
                    if (equaliser.equals(currRow, row)) {
                        // We found a duplcate row in the state, which means we do not
                        // want to emit it downstream and we bump up its count instead
                        entry.f1 = new Integer(count + 1);
                        values.set(i, entry);
                        seen = true;
                        break;
                    }
                    i++;
                }
                if (!seen) {
                    Tuple2<RowData, Integer> record = new Tuple2(row, new Integer(1));
                    values.add(record);
                    collector.collect(row);
                }
                break;

            case UPDATE_BEFORE:
            case DELETE:
                final int lastIndex = values.size() - 1;
                final int index = removeFirst(values, row);
                if (index == -1) {
                    // XXX(sergei): not a useful error message, suppress since it spams the log
                    LOG.debug(STATE_CLEARED_WARN_MSG);
                    return;
                }
                if (index == -2) {
                    // updated duplicate count, update state but do not emit
                    break;
                }
                if (values.isEmpty()) {
                    // Delete this row
                    row.setRowKind(DELETE);
                    collector.collect(row);
                } else if (index == lastIndex) {
                    // Last row has been removed, update to the second last one
                    Tuple2<RowData, Integer> entry = values.get(values.size() - 1);
                    RowData latestRow = entry.f0;
                    latestRow.setRowKind(UPDATE_AFTER);
                    collector.collect(latestRow);
                }
                break;
        }

        if (values.isEmpty()) {
            state.clear();
        } else {
            state.update(values);
        }
    }

    private int removeFirst(List<Tuple2<RowData, Integer>> values, RowData remove) {
        final Iterator<Tuple2<RowData, Integer>> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Tuple2<RowData, Integer> entry = iterator.next();
            RowData row = entry.f0;
            Integer count = entry.f1;
            // Ignore kind during comparison
            remove.setRowKind(row.getRowKind());
            if (equaliser.equals(row, remove)) {
                if (count == 1) {
                    iterator.remove();
                    return i;
                } else {
                    // update count only, do not trigger a re-emit
                    entry.f1 = new Integer(count - 1);
                    values.set(i, entry);
                    return -2;
                }
            }
            i++;
        }
        return -1;
    }
}
