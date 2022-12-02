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
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class OverAggregateFunction 
    extends KeyedProcessFunction<RowData, RowData, RowData>  {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OverAggregateFunction.class);

    private RecordEqualiser equaliser;
    private GeneratedRecordEqualiser generatedEqualiser;
    private final InternalTypeInfo<RowData> inputRowType;
    private final RowDataStringSerializer inputRowSerializer;

    private transient ValueState<RowData> accState;
    private transient ListState<RowData> listState;
    private final LogicalType[] accTypes;
    private transient AggsHandleFunction function = null;
    private PerKeyStateDataViewStore dataViewStore = null;
    private GeneratedAggsHandleFunction genAggsHandler;
    private final RecordCounter recordCounter;

    private JoinedRowData outputRow;

    // XXX(sergei): batch mode not supported yet
    private final boolean isStreamMode = true;

    // XXX(sergei): we do not support count(*) yet
    private final int indexOfCountStar = -1;

    public OverAggregateFunction(
            InternalTypeInfo<RowData> inputRowType,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            GeneratedRecordEqualiser generatedEqualiser
        ) {
        this.inputRowType = inputRowType;
        this.accTypes = accTypes;
        this.genAggsHandler = genAggsHandler;
        this.generatedEqualiser = generatedEqualiser;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.inputRowSerializer = new RowDataStringSerializer(this.inputRowType);
    }
            

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        accState = getRuntimeContext().getState(accDesc);

        dataViewStore = new PerKeyStateDataViewStore(getRuntimeContext());
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(dataViewStore);

        ListStateDescriptor<RowData> listStateDesc =
            new ListStateDescriptor<RowData>("listState", inputRowType);
        listState = getRuntimeContext().getListState(listStateDesc);

        // compile equaliser
        equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        generatedEqualiser = null;

        outputRow = new JoinedRowData();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {

        boolean firstRow;
        RowData accumulators = accState.value();
        if (null == accumulators) {
            // Don't create a new accumulator for a retraction message. This
            // might happen if the retraction message is the first message for the
            // key or after a state clean up.
            if (isRetractMsg(input)) {
                return;
            }
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            firstRow = false;
        }
        
        // set accumulators to handler first
        function.setAccumulators(accumulators);
        // get previous aggregate result
        RowData prevAggValue = function.getValue();
       
        // update aggregate result and set to the newRow
        if (isAccumulateMsg(input)) {
            // accumulate input
            function.accumulate(input);
        } else {
            // retract input
            function.retract(input);
        }
        // get current aggregate result
        RowData newAggValue = function.getValue();
        
        // get accumulator
        accumulators = function.getAccumulators();
        
        if (!recordCounter.recordCountIsZero(accumulators)) {
            // we aggregated at least one record for this key
            // update the state
            accState.update(accumulators);

            // if this was not the first row and we have to emit retractions
            if (!firstRow) {
                for (RowData row : listState.get()) {
                    outputRow
                        .replace(row, prevAggValue)
                        .setRowKind(RowKind.UPDATE_BEFORE);
                    collectIfNotBatch(out, outputRow);
                    outputRow.replace(row, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
                    collectIfNotBatch(out, outputRow);
                }
            } else {
                // this is the first, output new result
                // prepare INSERT message for new row
                outputRow.replace(input, newAggValue).setRowKind(RowKind.INSERT);
                collectIfNotBatch(out, outputRow);
            }
        } else {
            // we retracted the last record for this key
            // sent out a delete message
            if (!firstRow) {
                // prepare delete message for previous row
                outputRow.replace(input, prevAggValue).setRowKind(RowKind.DELETE);
                collectIfNotBatch(out, outputRow);
            }
            // and clear all state
            accState.clear();
            // cleanup dataview under current key
            function.cleanup();
        }

        // clobber row kind for comparisons
        input.setRowKind(RowKind.INSERT);

        if (isAccumulateMsg(input)) {
            outputRow.replace(input, newAggValue).setRowKind(RowKind.INSERT);
            collectIfNotBatch(out, outputRow);
            listState.add(input);
        } else {
            // we have already issued a retraction for this row in the loop above
            List<RowData> newState = new ArrayList<RowData>();
            for (RowData row : listState.get()) {
                if (!equaliser.equals(row, input)) {
                    newState.add(row);
                }
            }
            listState.update(newState);
        }
    }

    private void collectIfNotBatch(Collector<RowData> out, RowData output) {
        /* Supress emitting row if we're in batch mode */
        if (isStreamMode) {
            out.collect(output);
        }
    }
}
