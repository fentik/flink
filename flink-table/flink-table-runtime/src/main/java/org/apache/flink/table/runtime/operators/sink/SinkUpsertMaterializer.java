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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;



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
public class SinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SinkUpsertMaterializer.class);

    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. This will result in incorrect result. "
                    + "You can increase the state ttl to avoid this.";

    private final StateTtlConfig ttlConfig;
    private final TypeSerializer<RowData> serializer;
    private final GeneratedRecordEqualiser generatedEqualiser;

    private transient RecordEqualiser equaliser;
    private transient RowType physicalRowType;

    // Buffer of emitted insertions on which deletions will be applied first.
    // The row kind might be +I or +U and will be ignored when applying the deletion.
    private transient ValueState<List<RowData>> state;
    private transient TimestampedCollector<RowData> collector;

    public SinkUpsertMaterializer(
            StateTtlConfig ttlConfig,
            TypeSerializer<RowData> serializer,
            GeneratedRecordEqualiser generatedEqualiser) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedEqualiser = generatedEqualiser;
        this.physicalRowType = null;
        LOG.info("WARNING: SinkUpsertMaterializer is being called without the physicalRowType - This reduces what we can log for debugging. See the stacktrace to see if you can fix the caller.");
        String stacktrace = ExceptionUtils.stringifyException((new Exception("Missing RowType information")));
        LOG.info(stacktrace);
    }

    public SinkUpsertMaterializer(
        StateTtlConfig ttlConfig,
        TypeSerializer<RowData> serializer,
        GeneratedRecordEqualiser generatedEqualiser,
        RowType physicalRowType) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedEqualiser = generatedEqualiser;
        this.physicalRowType = physicalRowType;
        if (this.physicalRowType == null) {
            LOG.info("WARNING: SinkUpsertMaterializer is being called without the physicalRowType - This reduces what we can log for debugging. See the stacktrace to see if you can fix the caller.");
            String stacktrace = ExceptionUtils.stringifyException((new Exception("Missing RowType information")));
            LOG.info(stacktrace);
        } else {
            LOG.info("Initializing SinkUpsertMaterializer with schema for the input row: " + this.physicalRowType.asSummaryString());
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.equaliser =
                generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        ValueStateDescriptor<List<RowData>> descriptor =
                new ValueStateDescriptor<>("values", new ListSerializer<>(serializer));
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        this.state = getRuntimeContext().getState(descriptor);
        this.collector = new TimestampedCollector<>(output);
    }

    // TODO(akhilg): This is an exact copy of a function in AbstractStreamingOperator. When you are getting
    // bored, figure out which class/interface can be put this function in and re-use between these two classes.
    protected static String rowToString(RowType type, RowData row) {
        LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        int rowArity = type.getFieldCount();
        String rowString = "";
        for (int i = 0; i < rowArity; i++) {
            String value = "";
            if (row.isNullAt(i)) {
                value = "<NULL>";
            } else {
                switch (fieldTypes[i].getTypeRoot()) {
                        case NULL:
                            value = "<NULL>";
                            break;

                        case BOOLEAN:
                            value = row.getBoolean(i) ? "True" : "False";
                            break;

                        case INTEGER:
                        case INTERVAL_YEAR_MONTH:
                            value = Integer.toString(row.getInt(i));
                            break;

                        case BIGINT:
                        case INTERVAL_DAY_TIME:
                            value = Long.toString(row.getLong(i));
                            break;

                        case CHAR:
                        case VARCHAR:
                            value = row.getString(i).toString();
                            break;

                        case DATE:
                            value = "[DATE TYPE]";
                            break;

                        default:
                            value = "[Unprocessed type]";
                            break;
                }
            }
            String field = fieldNames[i] + "=" + value;
            rowString = rowString + field + (i == rowArity - 1 ? "" : ", ");
        }
        return rowString;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData row = element.getValue();
        List<RowData> values = state.value();
        if (values == null) {
            values = new ArrayList<>(2);
        }
        int size = -1;
        String key = row.getString(0).toString();  // Assumes the first column in a string.
        if (row instanceof BinaryRowData) {
            size = ((BinaryRowData)row).getSizeInBytes();
        }
        if (values.size() > 10 || size > 1024*1024) {
            LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: EXPENSIVE update to state value with key " + key + " to values {num entries: " + values.size() + "} with a row of size " + size);
        }
        if (this.shouldLogInput()) {
            if (this.physicalRowType != null) {
                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with key " + key + "(" + size + ")" + " to values {num entries: " + values.size() + "} : " +
                this.rowToString(this.physicalRowType, row));
            } else {
                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with key " + key + "(" + size + ")" + " to values {num entries: " + values.size() + "} : " + element.toString());
            }
        }
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                row.setRowKind(values.isEmpty() ? INSERT : UPDATE_AFTER);
                values.add(row);
                collector.collect(row);
                break;

            case UPDATE_BEFORE:
            case DELETE:
                final int lastIndex = values.size() - 1;
                final int index = removeFirst(values, row);
                if (index == -1) {
                    LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: WARNING: Did not find a value for " + key + ".We may have truncated the values too early.");
                    LOG.info(STATE_CLEARED_WARN_MSG);
                    return;
                }
                if (values.isEmpty()) {
                    // Delete this row
                    row.setRowKind(DELETE);
                    collector.collect(row);
                } else if (index == lastIndex) {
                    // Last row has been removed, update to the second last one
                    final RowData latestRow = values.get(values.size() - 1);
                    latestRow.setRowKind(UPDATE_AFTER);
                    collector.collect(latestRow);
                }
                break;
        }

        if (values.isEmpty()) {
            state.clear();
        } else {
            // We assume we are not going to get out-of-order entries for more than 20.
            if (value.size() > 50) {
                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Removing 30 entries for " + key + " currently with " + values.size() + " values. Hopefully, this doesn't cause issues.");
                removeN(values, 30);
            }
            state.update(values);
        }
    }

    private void removeN(List<RowData> values, int count) {
        for(int i = 0; i < count; i++) {
            values.remove(0);
        }
    }

    private int removeFirst(List<RowData> values, RowData remove) {
        final Iterator<RowData> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            final RowData row = iterator.next();
            // Ignore kind during comparison
            remove.setRowKind(row.getRowKind());
            if (equaliser.equals(row, remove)) {
                iterator.remove();
                return i;
            }
            i++;
        }
        return -1;
    }
}
