package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import java.io.IOException;

public class RetractableLagFunction
    extends KeyedProcessFunction<RowData, RowData, RowData>  {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RetractableLagFunction.class);
    private final int lagOffset;
    private final int inputFieldIdx;

    private final InternalTypeInfo<RowData> sortKeyType;
    private final InternalTypeInfo<RowData> inputRowType;
    private final RowDataStringSerializer sortKeySerializer;
    private final RowDataStringSerializer inputRowSerializer;
    private final TypeSerializer<RowData> inputRowSer;
    private final ComparableRecordComparator serializableComparator;
    private final KeySelector<RowData, RowData> sortKeySelector;

    private GeneratedRecordComparator generatedSortKeyComparator;
    private Comparator<RowData> sortKeyComparator;
    private RowData.FieldGetter lagFieldGetter;


    // a value state stores mapping from sort key to records list
    // XXX(sergei): this requires to serialize/deserialize on every CRUD
    // access for the state; unforutanately, Flink does not expose a
    // a SortedMapState interface in the backend (it's something on the
    // roadmap, but not in 1.16 or 1.15)
    private transient ValueState<SortedMap<RowData, List<RowData>>> dataState;

    private GeneratedRecordEqualiser generatedEqualiser;
    private RecordEqualiser equaliser;
    private JoinedRowData outputRow;

    public RetractableLagFunction(
            InternalTypeInfo<RowData> inputRowType,
            ComparableRecordComparator comparableRecordComparator,
            int lagOffset,
            int inputFieldIdx,
            RowDataKeySelector sortKeySelector,
            GeneratedRecordComparator generatedSortKeyComparator,
            GeneratedRecordEqualiser generatedEqualiser) {
        this.inputRowType = inputRowType;
        this.lagOffset = lagOffset;
        this.inputFieldIdx = inputFieldIdx;
        this.sortKeySelector = sortKeySelector;
        this.sortKeyType = sortKeySelector.getProducedType();
        this.sortKeySerializer = new RowDataStringSerializer(this.sortKeyType);
        this.inputRowSerializer = new RowDataStringSerializer(this.inputRowType);
        this.serializableComparator = comparableRecordComparator;
        this.generatedEqualiser = generatedEqualiser;
                this.generatedSortKeyComparator = generatedSortKeyComparator;
        this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
        this.lagFieldGetter = RowData.createFieldGetter(
            inputRowType.toRowType().getTypeAt(inputFieldIdx),
            inputFieldIdx);
    }
            

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // compile equaliser
        equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        generatedEqualiser = null;

        // compile comparator
        sortKeyComparator =
                generatedSortKeyComparator.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());
        generatedSortKeyComparator = null;

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        ValueStateDescriptor<SortedMap<RowData, List<RowData>>> valueStateDescriptor = new ValueStateDescriptor<>(
                "data-state",
                new SortedMapTypeInfo<>(
                        sortKeyType, valueTypeInfo, serializableComparator));

        dataState = getRuntimeContext().getState(valueStateDescriptor);

        outputRow = new JoinedRowData();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {

        SortedMap<RowData, List<RowData>> sortedMap = dataState.value();
        if (sortedMap == null) {
            sortedMap = new TreeMap<RowData, List<RowData>>(sortKeyComparator);
        }

        RowData sortKey = sortKeySelector.getKey(input);
        boolean isAccumulate = RowDataUtil.isAccumulateMsg(input);

        if (true) {
            BinaryRowData r = (BinaryRowData) sortKey;
            LOG.info("SERGEI sort key = {}", r.getInt(0));
        }

        List<RowData> records;
        if (sortedMap.containsKey(sortKey)) {
            records = sortedMap.get(sortKey);
        } else {
            records = new ArrayList<RowData>();
            sortedMap.put(sortKey, records);
        }

        LOG.info("SERGEI INPUT {}", inputRowSerializer.asString(input));


        RowData precedingRecord = null;
        RowData followingRecord = null;

        if (!records.isEmpty()) {
            // current sort key has existing entries, grab the last one
            // insert current row into the list
            precedingRecord = records.get(records.size() - 1);
        } else {
            // current sort key does not have existing entries, check to see
            // if there's a preceding entry
            // .   headMap(K toKey)
            // .   Returns a view of the portion of this map whose keys are strictly less than toKey.
            SortedMap<RowData, List<RowData>> prevMap = sortedMap.headMap(sortKey);
            if (!prevMap.isEmpty()) {
                final List<RowData> recs = prevMap.get(prevMap.lastKey());
                precedingRecord = recs.get(recs.size() - 1);
            }
        }

        // tailMap(K fromKey)
        // Returns a view of the portion of this map whose keys are greater than or equal to fromKey.
        SortedMap<RowData, List<RowData>> nextMap = sortedMap.tailMap(sortKey);
        LOG.info("SERGEI nextMap = {}", nextMap);
        if (!nextMap.isEmpty()) {
            for (final List<RowData> recs : nextMap.values()) {
                if (recs.isEmpty()) {
                    continue;
                }
                followingRecord = recs.get(0);
                break;
            }
        }

        if (precedingRecord != null) {
            LOG.info("SERGEI preceding {}", inputRowSerializer.asString(precedingRecord));
        }

        if (followingRecord != null) {
            LOG.info("SERGEI following {}", inputRowSerializer.asString(followingRecord));
        }

        if (isAccumulate) {
            LOG.info("SERGEI process addition");
            records.add(input);

            if (followingRecord != null) {
                // issue a retraction for the old following record and update it
                // with the new record state
                out.collect(buildOutputRow(followingRecord, precedingRecord, RowKind.UPDATE_BEFORE));
                out.collect(buildOutputRow(followingRecord, input, RowKind.UPDATE_AFTER));
            }

            out.collect(buildOutputRow(input, precedingRecord, RowKind.INSERT));
        } else {
            LOG.info("SERGEI process retract");
        }

        dataState.update(sortedMap);
    }

    private RowData buildOutputRow(RowData inputRow, RowData lagRow, RowKind kind) {
        GenericRowData lag = new GenericRowData(1);
        lag.setField(0, extractLagValue(lagRow));
        outputRow.replace(inputRow, lag);
        outputRow.setRowKind(kind);
        return outputRow;
    }

    private Object extractLagValue(RowData row) {
        if (row == null) {
            return null;
        }
        return lagFieldGetter.getFieldOrNull(row);
    }
}
