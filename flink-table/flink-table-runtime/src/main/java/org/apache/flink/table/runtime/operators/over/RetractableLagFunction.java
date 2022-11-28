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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
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

    private GeneratedRecordComparator generatedSortKeyComparator;
    protected Comparator<RowData> sortKeyComparator;


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
        this.sortKeyType = sortKeySelector.getProducedType();
        this.sortKeySerializer = new RowDataStringSerializer(this.sortKeyType);
        this.inputRowSerializer = new RowDataStringSerializer(this.inputRowType);
        this.serializableComparator = comparableRecordComparator;
        this.generatedEqualiser = generatedEqualiser;
        this.generatedSortKeyComparator = generatedSortKeyComparator;
        this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
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

        boolean isAccumulate = RowDataUtil.isAccumulateMsg(input);
        SortedMap<RowData, List<RowData>> sortedMap = dataState.value();
        if (sortedMap == null) {
            sortedMap = new TreeMap<RowData, List<RowData>>(sortKeyComparator);
        }

        if (isAccumulate) {
            LOG.info("SERGEI INPUT {}", inputRowSerializer.asString(input));
            outputAccumulateRow(out, input, new Integer(0));
        } else {
            LOG.info("SERGEI INPUT {}", inputRowSerializer.asString(input));
        }
    }

    private void outputAccumulateRow(Collector<RowData> out, RowData input, Object lagValue) {
        GenericRowData lagRow = new GenericRowData(1);
        lagRow.setField(0, lagValue);

        outputRow.replace(input, lagRow);
        outputRow.setRowKind(RowKind.UPDATE_AFTER);
        out.collect(outputRow);
    }
}
