package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.shaded.guava30.com.google.common.collect.SortedMultiset;
import org.apache.flink.shaded.guava30.com.google.common.collect.TreeMultiset;
import org.apache.flink.shaded.guava30.com.google.common.collect.BoundType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.table.runtime.typeutils.SortedMultisetTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;


public class OverAggregateFunction 
    extends KeyedProcessFunction<RowData, RowData, RowData>  {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(OverAggregateFunction.class);

    private RecordEqualiser equaliser;
    private GeneratedRecordEqualiser generatedEqualiser;
    private final InternalTypeInfo<RowData> inputRowType;
    private final RowDataStringSerializer inputRowSerializer;

    private ValueStateDescriptor<SortedMultiset<RowData>> stateDesc;
    private transient ValueState<SortedMultiset<RowData>> state;

    private final LogicalType[] accTypes;
    private transient AggsHandleFunction function = null;
    private PerKeyStateDataViewStore dataViewStore = null;
    private GeneratedAggsHandleFunction genAggsHandler;
    private final ComparableRecordComparator sortKeyComparator;
    private final Comparator<RowData> recordComparator;
    private final RecordCounter recordCounter;
    private final RowDataKeySelector sortKeySelector;

    private JoinedRowData outputRow;

    private boolean isStreamMode;

    // XXX(sergei): we do not support count(*) yet
    private final int indexOfCountStar = -1;

    public OverAggregateFunction(
            InternalTypeInfo<RowData> inputRowType,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            GeneratedRecordEqualiser generatedEqualiser,
            ComparableRecordComparator sortKeyComparator,
            RowDataKeySelector sortKeySelector,
            boolean isBatchBackfillEnabled
        ) {
        this.inputRowType = inputRowType;
        this.accTypes = accTypes;
        this.genAggsHandler = genAggsHandler;
        this.sortKeyComparator = sortKeyComparator;
        this.sortKeySelector = sortKeySelector;
        this.recordComparator = new KeyedRecordComparator(sortKeySelector, sortKeyComparator);
        this.generatedEqualiser = generatedEqualiser;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.inputRowSerializer = new RowDataStringSerializer(this.inputRowType);
        this.isStreamMode = !isBatchBackfillEnabled;

        SortedMultisetTypeInfo<RowData> smTypeInfo = new SortedMultisetTypeInfo<RowData>(inputRowType, recordComparator);
        this.stateDesc = new ValueStateDescriptor<SortedMultiset<RowData>>("state", smTypeInfo);
    }

    class KeyedRecordComparator implements Comparator<RowData>, Serializable {
        private final RowDataKeySelector sortKeySelector;
        private final ComparableRecordComparator sortKeyComparator;

        public KeyedRecordComparator(
                RowDataKeySelector sortKeySelector,
                ComparableRecordComparator sortKeyComparator) {
            this.sortKeyComparator = sortKeyComparator;
            this.sortKeySelector = sortKeySelector;
        }

        @Override
        public int compare(RowData r1, RowData r2) {
            try {
                RowData k1 = sortKeySelector.getKey(r1);
                RowData k2 = sortKeySelector.getKey(r2);
                return sortKeyComparator.compare(k1, k2);
            } catch (Exception e) {
                LOG.error("Unexpected exception {}", e);
                return 0;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        dataViewStore = new PerKeyStateDataViewStore(getRuntimeContext());
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(dataViewStore);

        state = getRuntimeContext().getState(stateDesc);

        // compile equaliser
        equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        generatedEqualiser = null;

        outputRow = new JoinedRowData();
    }

    @Override
    public boolean isHybridStreamBatchCapable() {
        return true;
    }

    public boolean isBatchMode() {
        return !this.isStreamMode;
    }

    private String getPrintableName() {
        return getRuntimeContext().getJobId() + " " + getRuntimeContext().getTaskName();
    }

    @Override
    public void emitStateAndSwitchToStreaming(Context ctx, Collector<RowData> out,
                    KeyedStateBackend<RowData> be) throws Exception {
        if (isStreamMode) {
            LOG.warn("Programming error in {} -- asked to switch to streaming while not in batch mode",
                        getPrintableName());
            return;
        }

        LOG.info("{} transitioning from Batch to Stream mode", getPrintableName());

        be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                stateDesc,
                new KeyedStateFunction<RowData, ValueState<SortedMultiset<RowData>>>() {
                    @Override
                    public void process(RowData key, ValueState<SortedMultiset<RowData>> state) throws Exception {
                        // The access to dataState.get() below requires a current key
                        // set for partioned stream operators.
                        be.setCurrentKey(key);

                        SortedMultiset<RowData> records = state.value();

                        RowData acc = function.createAccumulators();
                        function.setAccumulators(acc);

                        for (Iterator<RowData> i = records.iterator(); i.hasNext(); ) {
                            RowData record = i.next();
                            function.accumulate(record);
                            outputRow.setRowKind(RowKind.INSERT);
                            outputRow.replace(record, function.getValue());
                            out.collect(outputRow);
                        }
                    }
                });

        LOG.info("{} transitioned to Stream mode", getPrintableName());

        isStreamMode = true;
    }


    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        SortedMultiset<RowData> records = state.value();
        LOG.debug("SERGEI input {}", inputRowSerializer.asString(input));

        if (records == null) {
            records = TreeMultiset.create(recordComparator);
        }

        boolean isAccumulate = isAccumulateMsg(input);
        RowKind inputKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // clobber for comparisons

        RowData acc = null;

        // XXX(sergei): an inefficient implementation that's going after correctness
        // first; once we have that, we'll optimize for performance a bit
        //
        // The inefficient implementation looks as follows:
        //  - emit retraction for all rows in the current partition
        //  - add/remove the new row
        //  - emit inserts for all rows in the current partition

        if (isStreamMode) {
            acc = function.createAccumulators();
            function.setAccumulators(acc);

            for (Iterator<RowData> i = records.iterator(); i.hasNext(); ) {
                RowData record = i.next();
                function.accumulate(record);
                outputRow.setRowKind(RowKind.DELETE);
                outputRow.replace(record, function.getValue());
                out.collect(outputRow);
            }
        }

        if (isAccumulate) {
            records.add(input);
        } else {
            if (!records.remove(input)) {
                LOG.warn("row not found in state: {}", inputRowSerializer.asString(input));
            }
        }

        state.update(records);

        if (isStreamMode) {
            function.resetAccumulators();
            for (Iterator<RowData> i = records.iterator(); i.hasNext(); ) {
                RowData record = i.next();
                function.accumulate(record);
                outputRow.setRowKind(RowKind.INSERT);
                outputRow.replace(record, function.getValue());
                out.collect(outputRow);
            }
        }
    }
}
