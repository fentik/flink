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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.data.utils.JoinedRowData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility to create a {@link OuterJoinRecordStateViews} depends on
 * {@link JoinInputSideSpec}.
 */
public final class OuterJoinRecordStateViews {

    private static final Logger LOG = LoggerFactory.getLogger(OuterJoinRecordStateViews.class);

    /**
     * Creates a {@link OuterJoinRecordStateView} depends on
     * {@link JoinInputSideSpec}.
     */
    public static OuterJoinRecordStateView create(
            RuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            InternalTypeInfo<RowData> recordType,
            InternalTypeInfo<RowData> otherRecordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new OuterJoinRecordStateViews.JoinKeyContainsUniqueKey(
                        ctx, stateName, recordType, otherRecordType, ttlConfig);
            } else {
                return new OuterJoinRecordStateViews.InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        otherRecordType,
                        ttlConfig);
            }
        } else {
            return new OuterJoinRecordStateViews.InputSideHasNoUniqueKey(
                    ctx, stateName, recordType, otherRecordType, ttlConfig);
        }
    }

    // ------------------------------------------------------------------------------------------

    private static final class JoinKeyContainsUniqueKey implements OuterJoinRecordStateView {

        private final ValueState<Tuple2<RowData, Integer>> recordState;
        private final List<RowData> reusedRecordList;
        private final List<Tuple2<RowData, Integer>> reusedTupleList;
        private final String stateName;
        private final InternalTypeInfo<RowData> recordType;
        private final GenericRowData otherNullRow;

        private JoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> otherRecordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo = new TupleTypeInfo<>(recordType, Types.INT);
            ValueStateDescriptor<Tuple2<RowData, Integer>> recordStateDesc = new ValueStateDescriptor<>(stateName,
                    valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getState(recordStateDesc);
            // the result records always not more than 1
            this.reusedRecordList = new ArrayList<>(1);
            this.reusedTupleList = new ArrayList<>(1);
            this.stateName = stateName;
            this.recordType = recordType;
            this.otherNullRow = new GenericRowData(otherRecordType.toRowSize());
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            addRecord(record, -1);
        }

        @Override
        public void addRecord(RowData record, int numOfAssociations) throws Exception {
            recordState.update(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public void updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            recordState.update(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            recordState.clear();
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            Tuple2<RowData, Integer> tuple = recordState.value();
            if (tuple == null) {
                reusedRecordList.clear();
            } else {
                reusedRecordList.add(tuple.f0);
            }
            return reusedRecordList;
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations()
                throws Exception {
            reusedTupleList.clear();
            Tuple2<RowData, Integer> tuple = recordState.value();
            if (tuple != null) {
                reusedTupleList.add(tuple);
            }
            return reusedTupleList;
        }

        @Override
        public void emitCompleteState(KeyedStateBackend<RowData> be, Collector<RowData> collect,
                JoinRecordStateView otherView, JoinCondition condition) throws Exception {
            ValueStateDescriptor<RowData> recordStateDesc = new ValueStateDescriptor<>(stateName, recordType);

            JoinedRowData outRow = new JoinedRowData();
            outRow.setRowKind(RowKind.INSERT);

            be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                recordStateDesc,
                new KeyedStateFunction<RowData, ValueState<RowData>>() {
                    @Override
                    public void process(RowData key, ValueState<RowData> state) throws Exception {
                        RowData thisRow = state.value();

                        // set current key context for otherView fetch
                        be.setCurrentKey(key);

                        int rowsMatched = 0;
                        Iterable<RowData> records = otherView.getRecords();
                        for (RowData otherRow : records) {
                            boolean matched = condition.apply(thisRow, otherRow);
                            outRow.replace(thisRow, otherRow);
                            if (matched) {
                                collect.collect(outRow);
                                rowsMatched++;
                            }
                        }

                        if (rowsMatched == 0) {
                            outRow.replace(thisRow, otherNullRow);
                        }
                    }
                });
        }
    }

    private static final class InputSideHasUniqueKey implements OuterJoinRecordStateView {

        // stores record in the mapping <UK, <Record, associated-num>>
        private final MapState<RowData, Tuple2<RowData, Integer>> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;
        private final String stateName;
        private final InternalTypeInfo<RowData> recordType;
        private final InternalTypeInfo<RowData> uniqueKeyType;
        private final GenericRowData otherNullRow;

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> uniqueKeyType,
                KeySelector<RowData, RowData> uniqueKeySelector,
                InternalTypeInfo<RowData> otherRecordType,
                StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo = new TupleTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc = new MapStateDescriptor<>(stateName,
                    uniqueKeyType, valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
            this.stateName = stateName;
            this.recordType = recordType;
            this.uniqueKeyType = uniqueKeyType;
            this.otherNullRow = new GenericRowData(otherRecordType.toRowSize());
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            addRecord(record, -1);
        }

        @Override
        public void addRecord(RowData record, int numOfAssociations) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.put(uniqueKey, Tuple2.of(record, numOfAssociations));
        }

        @Override
        public void updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.put(uniqueKey, Tuple2.of(record, numOfAssociations));
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.remove(uniqueKey);
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return new RecordsIterable(getRecordsAndNumOfAssociations());
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations()
                throws Exception {
            return recordState.values();
        }

        @Override
        public void emitCompleteState(KeyedStateBackend<RowData> be, Collector<RowData> collect,
                JoinRecordStateView otherView, JoinCondition condition) throws Exception {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo = new TupleTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc = new MapStateDescriptor<>(stateName,
                    uniqueKeyType, valueTypeInfo);

            JoinedRowData outRow = new JoinedRowData();
            outRow.setRowKind(RowKind.INSERT);

            be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                recordStateDesc,
                new KeyedStateFunction<RowData, MapState<RowData, Tuple2<RowData, Integer>>>() {
                    @Override
                    public void process(RowData key, MapState<RowData, Tuple2<RowData, Integer>> state) throws Exception {
                        // set current key context for otherView fetch
                        be.setCurrentKey(key);

                        for (Map.Entry<RowData, Tuple2<RowData, Integer>> entry : state.entries()) {
                            RowData thisRow = entry.getValue().f0;
                            Iterable<RowData> records = otherView.getRecords();
                            int rowsMatched = 0;
                            for (RowData otherRow: records) {
                                boolean matched = condition.apply(thisRow, otherRow);

                                if (matched) {
                                    outRow.replace(thisRow, otherRow);
                                    collect.collect(outRow);
                                    rowsMatched++;
                                }
                            }
                            if (rowsMatched == 0) {
                                outRow.replace(thisRow, otherNullRow);
                            }
                        }
                    }
                });
        }
    }

    private static final class InputSideHasNoUniqueKey implements OuterJoinRecordStateView {

        // stores record in the mapping <Record, <appear-times, associated-num>>
        private final MapState<RowData, Tuple2<Integer, Integer>> recordState;
        private final String stateName;
        private final InternalTypeInfo<RowData> recordType;
        private final RowData otherNullRow;

        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> otherRecordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<Integer, Integer>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT, Types.INT);
            MapStateDescriptor<RowData, Tuple2<Integer, Integer>> recordStateDesc = new MapStateDescriptor<>(stateName,
                    recordType, tupleTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.stateName = stateName;
            this.recordType = recordType;
            this.otherNullRow = new GenericRowData(otherRecordType.toRowSize());
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            addRecord(record, -1);
        }

        @Override
        public void addRecord(RowData record, int numOfAssociations) throws Exception {
            Tuple2<Integer, Integer> tuple = recordState.get(record);
            if (tuple != null) {
                tuple.f0 = tuple.f0 + 1;
                tuple.f1 = numOfAssociations;
            } else {
                tuple = Tuple2.of(1, numOfAssociations);
            }
            recordState.put(record, tuple);
        }

        @Override
        public void updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            Tuple2<Integer, Integer> tuple = recordState.get(record);
            if (tuple != null) {
                tuple.f1 = numOfAssociations;
            } else {
                // compatible for state ttl
                tuple = Tuple2.of(1, numOfAssociations);
            }
            recordState.put(record, tuple);
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            Tuple2<Integer, Integer> tuple = recordState.get(record);
            if (tuple != null) {
                if (tuple.f0 > 1) {
                    tuple.f0 = tuple.f0 - 1;
                    recordState.put(record, tuple);
                } else {
                    recordState.remove(record);
                }
            }
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return new RecordsIterable(getRecordsAndNumOfAssociations());
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations()
                throws Exception {
            return new IterableIterator<Tuple2<RowData, Integer>>() {

                private final Iterator<Map.Entry<RowData, Tuple2<Integer, Integer>>> backingIterable = recordState
                        .entries().iterator();
                private Tuple2<RowData, Integer> tuple;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    return backingIterable.hasNext() || remainingTimes > 0;
                }

                @Override
                public Tuple2<RowData, Integer> next() {
                    if (remainingTimes > 0) {
                        checkNotNull(tuple);
                        remainingTimes--;
                        return tuple;
                    } else {
                        Map.Entry<RowData, Tuple2<Integer, Integer>> entry = backingIterable.next();
                        tuple = Tuple2.of(entry.getKey(), entry.getValue().f1);
                        remainingTimes = entry.getValue().f0 - 1;
                        return tuple;
                    }
                }

                @Override
                public Iterator<Tuple2<RowData, Integer>> iterator() {
                    return this;
                }
            };
        }

        @Override
        public void emitCompleteState(KeyedStateBackend<RowData> be, Collector<RowData> collect,
                JoinRecordStateView otherView, JoinCondition condition) throws Exception {
            TupleTypeInfo<Tuple2<Integer, Integer>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT, Types.INT);
            MapStateDescriptor<RowData, Tuple2<Integer, Integer>> recordStateDesc = new MapStateDescriptor<>(stateName,
                    recordType, tupleTypeInfo);

            JoinedRowData outRow = new JoinedRowData();
            outRow.setRowKind(RowKind.INSERT);

            be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                recordStateDesc,
                new KeyedStateFunction<RowData, MapState<RowData, Tuple2<Integer, Integer>>>() {
                    @Override
                    public void process(RowData key, MapState<RowData, Tuple2<Integer, Integer>> state) throws Exception {
                        // set current key context for otherView fetch
                        be.setCurrentKey(key);

                        for (Map.Entry<RowData, Tuple2<Integer, Integer>> entry : state.entries()) {
                            RowData thisRow = entry.getKey();
                            Iterable<RowData> records = otherView.getRecords();
                            int rowsMatched = 0;
                            for (RowData otherRow: records) {
                                boolean matched = condition.apply(thisRow, otherRow);

                                if (matched) {
                                    outRow.replace(thisRow, otherRow);
                                    collect.collect(outRow);
                                    rowsMatched++;
                                }
                            }
                            if (rowsMatched == 0) {
                                outRow.replace(thisRow, otherNullRow);
                            }
                        }
                    }
                });
        }
    }

    // ----------------------------------------------------------------------------------------

    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final Iterator<Tuple2<RowData, Integer>> tupleIterator;

        private RecordsIterable(Iterable<Tuple2<RowData, Integer>> tuples) {
            checkNotNull(tuples);
            this.tupleIterator = tuples.iterator();
        }

        @Override
        public Iterator<RowData> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return tupleIterator.hasNext();
        }

        @Override
        public RowData next() {
            return tupleIterator.next().f0;
        }
    }
}
