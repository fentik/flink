package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import java.util.Map;
import org.apache.flink.util.IterableIterator;
import static org.apache.flink.util.Preconditions.checkNotNull;
import java.util.Iterator;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.util.RowDataStringSerializer;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

public class BroadcastStreamingJoin extends KeyedBroadcastProcessFunction<RowData, RowData, RowData, JoinedRowData> {

    private final class NonBroadcastStateView {

        private final MapState<RowData, Integer> recordState;
        private final InternalTypeInfo<RowData> recordType;
        private final String stateName;
    
        private NonBroadcastStateView(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType) {
            MapStateDescriptor<RowData, Integer> recordStateDesc = new MapStateDescriptor<>(stateName, recordType, Types.INT);
            this.stateName = stateName;
            this.recordType = recordType;
            this.recordState = ctx.getMapState(recordStateDesc);
        }
    
        public void addRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                cnt += 1;
            } else {
                cnt = 1;
            }
            recordState.put(record, cnt);
        }
    
        public void retractRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                if (cnt > 1) {
                    recordState.put(record, cnt - 1);
                } else {
                    recordState.remove(record);
                }
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastStreamingJoin.class);
    private final MapStateDescriptor<RowData, Integer> broadcastStateDescriptor;
    private final GeneratedJoinCondition generatedJoinCondition;
    private transient JoinedRowData outRow;
    protected transient JoinCondition joinCondition;
    private transient NonBroadcastStateView nonBroadcastStateView;
    protected final InternalTypeInfo<RowData> nonBroadcastType;
    protected final InternalTypeInfo<RowData> broadcastType;
    private final boolean leftIsBroadcast;
    private transient RowDataStringSerializer nonBroadcastRecordSerializer;
    private transient RowDataStringSerializer broadcastRecordSerializer;
    private final RowDataKeySelector nonBroadcastJoinKeySelector;
    private final RowDataKeySelector broadcastJoinKeySelector;
    private final RowDataKeySelector broadcastStateKeySelector;
    private final RowDataKeySelector nonBroadcastStateKeySelector;


    public BroadcastStreamingJoin(
        MapStateDescriptor<RowData, Integer> broadcastStateDescriptor,
        GeneratedJoinCondition generatedJoinCondition,
        InternalTypeInfo<RowData> nonBroadcastType,
        InternalTypeInfo<RowData> broadcastType,
        boolean leftIsBroadcast,
        RowDataKeySelector nonBroadcastJoinKeySelector,
        RowDataKeySelector broadcastJoinKeySelector,
        RowDataKeySelector broadcastStateKeySelector,
        RowDataKeySelector nonBroadcastStateKeySelector
    ) {
        this.broadcastStateDescriptor = broadcastStateDescriptor;
        this.generatedJoinCondition = generatedJoinCondition;
        this.nonBroadcastType = nonBroadcastType;
        this.broadcastType = broadcastType;
        this.leftIsBroadcast = leftIsBroadcast;
        this.nonBroadcastJoinKeySelector = nonBroadcastJoinKeySelector;
        this.broadcastJoinKeySelector = broadcastJoinKeySelector;
        this.broadcastStateKeySelector = broadcastStateKeySelector;
        this.nonBroadcastStateKeySelector = nonBroadcastStateKeySelector;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // super.open(parameters);
        this.outRow = new JoinedRowData();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = condition;
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(parameters);
        this.nonBroadcastStateView = new NonBroadcastStateView(getRuntimeContext(), "non-broadcast-records", nonBroadcastType);
        this.nonBroadcastRecordSerializer = new RowDataStringSerializer(nonBroadcastType);
        this.broadcastRecordSerializer = new RowDataStringSerializer(broadcastType);
        
    }

    private Iterable<RowData> getBroadcastStateRecords(ReadOnlyContext ctx) throws Exception {
        return new IterableIterator<RowData>() {
            ReadOnlyBroadcastState<RowData, Integer> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
            private final Iterator<Map.Entry<RowData, Integer>> backingIterable = broadcastState.immutableEntries().iterator();
            private RowData record;
            private int remainingTimes = 0;

            @Override
            public boolean hasNext() {
                return backingIterable.hasNext() || remainingTimes > 0;
            }

            @Override
            public RowData next() {
                if (remainingTimes > 0) {
                    checkNotNull(record);
                    remainingTimes--;
                    return record;
                } else {
                    Map.Entry<RowData, Integer> entry = backingIterable.next();
                    record = entry.getKey();
                    remainingTimes = entry.getValue() - 1;
                    return record;
                }
            }

            @Override
            public Iterator<RowData> iterator() {
                return this;
            }
        };
    }
    
    @Override
    public void processElement(RowData value, ReadOnlyContext ctx, Collector<JoinedRowData> out) throws Exception{
        LOG.info("processElement {}", nonBroadcastRecordSerializer.asString(value));
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(value);
        for (RowData record: getBroadcastStateRecords(ctx)) {
            boolean matched = nonBroadcastJoinKeySelector.getKey(value).equals(broadcastJoinKeySelector.getKey(record));
            if (matched) {
                LOG.info("[Non broadcast] Record matched {} {}", nonBroadcastRecordSerializer.asString(value), broadcastRecordSerializer.asString(record));
                if (leftIsBroadcast) {
                    outRow.replace(record, value);
                } else {
                    outRow.replace(value, record);
                }
                out.collect(outRow);
            }
        }
        if (isAccumulateMsg) {
            nonBroadcastStateView.addRecord(value);
        } else {
            nonBroadcastStateView.retractRecord(value);
        }
    }

    @Override
    public void processBroadcastElement(RowData value, Context ctx, Collector<JoinedRowData> out) throws Exception {
        LOG.info("processBroadcastElement {}", broadcastRecordSerializer.asString(value));
        MapStateDescriptor<RowData, Integer> recordStateDesc = new MapStateDescriptor<>(
            "non-broadcast-records",
            nonBroadcastType,
            Types.INT);
        if (!ctx.getKeyedStateBackend().toString().contains("RocksDBKeyedStateBackend")) {
            throw new Exception("Broadcast streaming joins are only supported when backend is RocksDB. Backend = " + ctx.getKeyedStateBackend().toString());
        }
        // if (broadcastStateKeySelector.getKey(value).equals(broadcastJoinKeySelector.getKey(value))) {
        //     throw new Exception("Key from state key selector should be different than key from join key selector");
        // }
        ctx.applyToKeyedStateWithPrefix(
            recordStateDesc,
            new KeyedStateFunction<RowData, MapState<RowData, Integer>>() {
                @Override
                public void process(RowData key, MapState<RowData, Integer> state) throws Exception {
                    for (Map.Entry<RowData, Integer> entry : state.entries()) {
                        RowData record = entry.getKey();
                        final DataOutputSerializer joinKeyOutputView = new DataOutputSerializer(128);
                        final DataOutputSerializer stateKeyOutputView = new DataOutputSerializer(128);
                        final DataOutputSerializer prefixOutputView = new DataOutputSerializer(128);
                        CompositeKeySerializationUtils.writeKey(
                            nonBroadcastJoinKeySelector.getKey(record),
                            ctx.getKeyedStateBackend().getKeySerializer(),
                            joinKeyOutputView,
                            false
                        );

                        CompositeKeySerializationUtils.writeKey(
                            nonBroadcastStateKeySelector.getKey(record),
                            ctx.getKeyedStateBackend().getKeySerializer(),
                            stateKeyOutputView,
                            false
                        );

                        CompositeKeySerializationUtils.writeKey(
                            broadcastStateKeySelector.getKey(value),
                            ctx.getKeyedStateBackend().getKeySerializer(),
                            prefixOutputView,
                            false);
                        
                        LOG.info("[BYTE STATE] joinKeyOnLeft {} stateKeyonLeft {} prefix {}", joinKeyOutputView.getCopyOfBuffer(), stateKeyOutputView.getCopyOfBuffer(), prefixOutputView.getCopyOfBuffer());
                        // LOG.info("[Broadcast] Checking for match {} {}", broadcastRecordSerializer.asString(value), nonBroadcastRecordSerializer.asString(record));
                        boolean matched = broadcastJoinKeySelector.getKey(value).equals(nonBroadcastJoinKeySelector.getKey(record));
                        if (matched) {
                            LOG.info("[Broadcast] Matched {} {}", broadcastRecordSerializer.asString(value), nonBroadcastRecordSerializer.asString(record));
                            if (leftIsBroadcast) {
                                outRow.replace(value, record);
                            } else {
                                outRow.replace(record, value);
                            }
                            out.collect(outRow);
                        }
                    }
                }},
            broadcastJoinKeySelector.getKey(value)
        );

        BroadcastState<RowData, Integer> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(value);
        if (isAccumulateMsg) {
            Integer cnt = broadcastState.get(value);
            if (cnt != null) {
                cnt += 1;
            } else {
                cnt = 1;
            }
            broadcastState.put(value, cnt);
        } else {
            Integer cnt = broadcastState.get(value);
            if (cnt != null) {
                if (cnt > 1) {
                    broadcastState.put(value, cnt - 1);
                } else {
                    broadcastState.remove(value);
                }
            }
        }
    }

} 