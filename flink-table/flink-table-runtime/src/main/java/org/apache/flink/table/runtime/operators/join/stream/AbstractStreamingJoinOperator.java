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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.flink.types.RowKind;
import org.apache.flink.types.Row;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamingJoinOperator.class);
    private static final long serialVersionUID = -376944622236540545L;

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final GeneratedJoinCondition generatedJoinCondition;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;

    protected final JoinInputSideSpec leftInputSideSpec;
    protected final JoinInputSideSpec rightInputSideSpec;

    private final boolean[] filterNullKeys;

    protected final long stateRetentionTime;

    protected transient JoinConditionWithNullFilters joinCondition;
    protected transient TimestampedCollector<RowData> collector;

    public AbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long stateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.stateRetentionTime = stateRetentionTime;
        this.filterNullKeys = filterNullKeys;
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(new Configuration());

        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    protected static String rowToString(InternalTypeInfo<RowData> internalTypes, RowData row) {
        RowType type = internalTypes.toRowType();
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

    /**
     * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper of
     * {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and {@link
     * #getOuterRecords()}. See the method Javadoc for more details.
     */
    protected static final class AssociatedRecords {
        private final List<OuterRecord> records;

        private AssociatedRecords(List<OuterRecord> records) {
            checkNotNull(records);
            this.records = records;
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

        public int size() {
            return records.size();
        }

        /**
         * Gets the iterable of records. This is usually be called when the {@link
         * AssociatedRecords} is from inner side.
         */
        public Iterable<RowData> getRecords() {
            return new RecordsIterable(records);
        }

        /**
         * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations.
         * This is usually be called when the {@link AssociatedRecords} is from outer side.
         */
        public Iterable<OuterRecord> getOuterRecords() {
            return records;
        }

        /**
         * Creates an {@link AssociatedRecords} which represents the records associated to the input
         * row.
         */

	public static AssociatedRecords of(
                RowData input,
                boolean inputIsLeft,
                JoinRecordStateView otherSideStateView,
                JoinCondition condition)
                throws Exception {
	    String operator_name = " ";
	    return AssociatedRecords.of(input, inputIsLeft, null, null, operator_name, otherSideStateView, condition);
	}

        public static AssociatedRecords of(
                RowData input,
                boolean inputIsLeft,
		InternalTypeInfo<RowData> leftType,
		InternalTypeInfo<RowData> rightType,
		String operator_name,
                JoinRecordStateView otherSideStateView,
                JoinCondition condition)
                throws Exception {
            List<OuterRecord> associations = new ArrayList<>();
	    int rows_fetched = 0;
	    int rows_matched = 0;
            if (otherSideStateView instanceof OuterJoinRecordStateView) {
                OuterJoinRecordStateView outerStateView =
                        (OuterJoinRecordStateView) otherSideStateView;
                Iterable<Tuple2<RowData, Integer>> records =
                        outerStateView.getRecordsAndNumOfAssociations();
                for (Tuple2<RowData, Integer> record : records) {
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record.f0)
                                    : condition.apply(record.f0, input);
		    rows_fetched = rows_fetched + 1;
                    if (matched) {
			rows_matched = rows_matched + 1;
                        associations.add(new OuterRecord(record.f0, record.f1));
                    }
                }
		if ((rows_fetched > 1000 || rows_fetched - rows_matched > 500) && leftType != null && rightType != null) {
		    LOG.info(operator_name + ": EXPENSIVE Outer Join fetched: " + rows_fetched + ", matched " + rows_matched);
		    LOG.info(operator_name + ": EXPENSIVE joining " + (inputIsLeft ? " left input: " : "right input: ") + rowToString(inputIsLeft ? leftType : rightType, input));
		}
            } else {
                Iterable<RowData> records = otherSideStateView.getRecords();
                for (RowData record : records) {
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record)
                                    : condition.apply(record, input);
		    rows_fetched = rows_fetched + 1;

                    if (matched) {
			rows_matched = rows_matched + 1;
                        // use -1 as the default number of associations
                        associations.add(new OuterRecord(record, -1));
                    }
                }
		if ((rows_fetched > 1000 || rows_fetched - rows_matched > 500)  && leftType != null && rightType != null) {
		    LOG.info(operator_name + ": EXPENSIVE Inner Join fetched: " + rows_fetched + ", matched " + rows_matched);
		    LOG.info(operator_name + ": EXPENSIVE Joining " + (inputIsLeft ? " left input: " : "right input: ") + rowToString(inputIsLeft ? leftType : rightType, input));
                }
            }
            return new AssociatedRecords(associations);
        }
    }

    /** A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<RowData>}. */
    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final List<OuterRecord> records;
        private int index = 0;

        private RecordsIterable(List<OuterRecord> records) {
            this.records = records;
        }

        @Override
        public Iterator<RowData> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public RowData next() {
            RowData row = records.get(index).record;
            index++;
            return row;
        }
    }

    /**
     * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
     * numOfAssociations} represents the number of associated records in the other side. It is used
     * when the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
     * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid
     * recompute the associated numbers every time.
     *
     * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
     * numOfAssociations} will always be {@code -1}.
     */
    protected static final class OuterRecord {
        public final RowData record;
        public final int numOfAssociations;

        private OuterRecord(RowData record, int numOfAssociations) {
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }
    }
}
