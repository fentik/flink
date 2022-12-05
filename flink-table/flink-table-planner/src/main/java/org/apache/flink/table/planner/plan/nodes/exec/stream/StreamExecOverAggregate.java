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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.runtime.operators.over.RetractableLagFunction;
import org.apache.flink.table.runtime.operators.over.OverAggregateFunction;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for time-based over operator. */
@ExecNodeMetadata(
        name = "stream-exec-over-aggregate",
        version = 1,
        producedTransformations = StreamExecOverAggregate.OVER_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecOverAggregate.class);

    public static final String OVER_AGGREGATE_TRANSFORMATION = "over-aggregate";

    public static final String FIELD_NAME_OVER_SPEC = "overSpec";

    @JsonProperty(FIELD_NAME_OVER_SPEC)
    private final OverSpec overSpec;

    // XXX(sergei): parameterize this
    private final boolean useGenericOverAggregate = true;

    public StreamExecOverAggregate(
            ReadableConfig tableConfig,
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecOverAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecOverAggregate.class, tableConfig),
                overSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecOverAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_OVER_SPEC) OverSpec overSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.overSpec = checkNotNull(overSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (overSpec.getGroups().size() > 1) {
            throw new TableException("All aggregates must be computed on the same window.");
        }

        final OverSpec.GroupSpec group = overSpec.getGroups().get(0);

        final KeyedProcessFunction<RowData, RowData, RowData> overProcessFunction;

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputRowType);

        final List<RexLiteral> constants = overSpec.getConstants();
        final List<String> fieldNames = new ArrayList<>(inputRowType.getFieldNames());
        final List<LogicalType> fieldTypes = new ArrayList<>(inputRowType.getChildren());
        IntStream.range(0, constants.size()).forEach(i -> fieldNames.add("TMP" + i));
        for (int i = 0; i < constants.size(); ++i) {
            fieldNames.add("TMP" + i);
            fieldTypes.add(FlinkTypeFactory.toLogicalType(constants.get(i).getType()));
        }

        final RowType aggInputRowType =
                RowType.of(
                        fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        final CodeGeneratorContext ctx = new CodeGeneratorContext(config.getTableConfig());


        // XXX(sergei): the way I detect a LAG function here is really Janky, but that's
        // OK for now. The code will create a process function that looks very similar
        // to the RetractableTopNFunction implementation for the RANK operator. There's
        // a lot of similarity to codepath here.
        int numLagAggs = 0;
        for (AggregateCall agg : group.getAggCalls()) {
            if (agg.getAggregation().getName() == "LAG") {
                numLagAggs++;
            }
        }
        if (numLagAggs > 0) {
            // XXX(sergei): need to merge the two implementation, but for now keep things
            // simpler and create two separate code paths
            if (numLagAggs != group.getAggCalls().size()) {
                throw new TableException("Mixing LAG and non-LAG aggregate functions is not supported yet.");
            }
            overProcessFunction = createRetractableLagFunction(
                                    ctx,
                                    config,
                                    group.getAggCalls(),
                                    constants,
                                    group.getSort(),
                                    inputRowType,
                                    aggInputRowType,
                                    inputRowTypeInfo);
        } else if (useGenericOverAggregate) {
            overProcessFunction = createGenericOverAggregateFunction(
                                    ctx,
                                    config,
                                    group.getAggCalls(),
                                    constants,
                                    group.getSort(),
                                    inputRowType,
                                    aggInputRowType,
                                    inputRowTypeInfo,
                                    planner.getRelBuilder());
        } else {
            final int[] orderKeys = group.getSort().getFieldIndices();
            final boolean[] isAscendingOrders = group.getSort().getAscendingOrders();
            final int orderKey = orderKeys[0];
            final LogicalType orderKeyType = inputRowType.getFields().get(orderKey).getType();
    

            if (orderKeys.length != 1 || isAscendingOrders.length != 1) {
                throw new TableException("The window can only be ordered by a single time column.");
            }

            if (!isAscendingOrders[0]) {
                throw new TableException("The window can only be ordered in ASCENDING mode.");
            }

            if (partitionKeys.length > 0 && config.getStateRetentionTime() < 0) {
                LOG.warn(
                        "No state retention interval configured for a query which accumulates state. "
                                + "Please provide a query configuration with valid retention interval to prevent "
                                + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
            }


            // check time field && identify window rowtime attribute
            final int rowTimeIdx;
            if (isRowtimeAttribute(orderKeyType)) {
                rowTimeIdx = orderKey;
            } else if (isProctimeAttribute(orderKeyType)) {
                rowTimeIdx = -1;
            } else {
                throw new TableException(
                        "OVER windows' ordering in stream mode must be defined on a time attribute instead of " + orderKeyType);
            }

            if (group.getLowerBound().isPreceding()
                    && group.getLowerBound().isUnbounded()
                    && group.getUpperBound().isCurrentRow()) {
                // unbounded OVER window
                overProcessFunction =
                        createUnboundedOverProcessFunction(
                                ctx,
                                group.getAggCalls(),
                                constants,
                                aggInputRowType,
                                inputRowType,
                                rowTimeIdx,
                                group.isRows(),
                                config,
                                planner.getRelBuilder());
            } else if (group.getLowerBound().isPreceding()
                    && !group.getLowerBound().isUnbounded()
                    && group.getUpperBound().isCurrentRow()) {
                final Object boundValue =
                        OverAggregateUtil.getBoundary(overSpec, group.getLowerBound());

                if (boundValue instanceof BigDecimal) {
                    throw new TableException(
                            "the specific value is decimal which haven not supported yet.");
                }
                // bounded OVER window
                final long precedingOffset = -1 * (long) boundValue + (group.isRows() ? 1 : 0);
                overProcessFunction =
                        createBoundedOverProcessFunction(
                                ctx,
                                group.getAggCalls(),
                                constants,
                                aggInputRowType,
                                inputRowType,
                                rowTimeIdx,
                                group.isRows(),
                                precedingOffset,
                                config,
                                planner.getRelBuilder());
            } else {
                throw new TableException("OVER RANGE FOLLOWING windows are not supported yet.");
            }
        }

        final KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(overProcessFunction);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(OVER_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        partitionKeys, InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }

    private KeyedProcessFunction<RowData, RowData, RowData> createGenericOverAggregateFunction(
                CodeGeneratorContext ctx,
                ExecNodeConfig config,
                List<AggregateCall> aggCalls,
                List<RexLiteral> constants,
                SortSpec sortSpec,
                RowType inputRowType,
                RowType aggInputRowType,
                InternalTypeInfo<RowData> inputRowTypeInfo,
                RelBuilder relBuilder) {

        final boolean isBatchBackfillEnabled = config.get(ExecutionConfigOptions.TABLE_EXEC_BATCH_BACKFILL);

        boolean[] aggCallNeedRetractions = new boolean[aggCalls.size()];
        Arrays.fill(aggCallNeedRetractions, true);

        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        aggCallNeedRetractions,
                        true, // needRetraction
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField
    
        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needAccumulate()
                        .needRetract()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("GenericOverAggregateHelper", aggInfoList);
    
        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        EqualiserCodeGenerator equaliserCodeGen =
                new EqualiserCodeGenerator(
                        inputRowType.getFields().stream()
                                .map(RowType.RowField::getType)
                                .toArray(LogicalType[]::new));
        
        GeneratedRecordEqualiser generatedEqualiser =
                equaliserCodeGen.generateRecordEqualiser("OverAggregateEqualiser");

        return new OverAggregateFunction(
                inputRowTypeInfo,
                genAggsHandler,
                flattenAccTypes,
                generatedEqualiser,
                isBatchBackfillEnabled);
    }

    private KeyedProcessFunction<RowData, RowData, RowData> createRetractableLagFunction(
                CodeGeneratorContext ctx,
                ExecNodeConfig config,
                List<AggregateCall> aggCalls,
                List<RexLiteral> constants,
                SortSpec sortSpec,
                RowType inputRowType,
                RowType aggInputRowType,
                InternalTypeInfo<RowData> inputRowTypeInfo) {
        LOG.debug("SERGEI create retractable lag function");

        LOG.debug("SERGEI inputRowType {}", inputRowType);
        LOG.debug("SERGEI aggInputRowType {}", aggInputRowType);
        LOG.debug("SERGEI aggCalls {}", aggCalls);
        LOG.debug("SERGEI contants {}", constants);

        List<Integer> inputFieldIdxs = new ArrayList<Integer>();

        // XXX(sergei): our implementation does not support custom offsets yet
        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.getArgList().size() != 1) {
                throw new TableException("LAG(expression) is currently the only supported syntax (offset=1 and default NULL).");
            }

            if (constants.size() > 0) {
                throw new TableException("LAG() does not currently accept constant arguments.");
            }

            // XXX(sergei): first argument is the index into the input row
            inputFieldIdxs.add(Integer.valueOf(aggCall.getArgList().get(0)));
        }

        // XXX(sergei): hardcode lag offset to 1 until above is resolved
        final int lagOffset = 1;

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);

        int[] sortFields = sortSpec.getFieldIndices();
        RowDataKeySelector sortKeySelector =
                KeySelectorUtil.getRowDataSelector(sortFields, inputRowTypeInfo);
        // create a sort spec on sort keys.
        int[] sortKeyPositions = IntStream.range(0, sortFields.length).toArray();
        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        IntStream.range(0, sortFields.length)
                .forEach(
                        idx ->
                                builder.addField(
                                        idx,
                                        sortSpec.getFieldSpec(idx).getIsAscendingOrder(),
                                        sortSpec.getFieldSpec(idx).getNullIsLast()));
        SortSpec sortSpecInSortKey = builder.build();
        GeneratedRecordComparator sortKeyComparator =
                ComparatorCodeGenerator.gen(
                        config.getTableConfig(),
                        "StreamExecOverAggregateOrderByComparator",
                        RowType.of(sortSpec.getFieldTypes(inputRowType)),
                        sortSpecInSortKey);

        EqualiserCodeGenerator equaliserCodeGen =
                new EqualiserCodeGenerator(
                         inputRowType.getFields().stream()
                                 .map(RowType.RowField::getType)
                                 .toArray(LogicalType[]::new));

        GeneratedRecordEqualiser generatedEqualiser =
                equaliserCodeGen.generateRecordEqualiser("LagValueEqualiser");

        ComparableRecordComparator comparator =
                new ComparableRecordComparator(
                        sortKeyComparator,
                        sortKeyPositions,
                        sortSpec.getFieldTypes(inputRowType),
                        sortSpec.getAscendingOrders(),
                        sortSpec.getNullsIsLast());

        return new RetractableLagFunction(
                        inputRowTypeInfo,
                        comparator,
                        inputFieldIdxs,
                        sortKeySelector,
                        sortKeyComparator,
                        generatedEqualiser);
    }

    /**
     * Create an ProcessFunction for unbounded OVER window to evaluate final aggregate value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputRowType physical type of the input row which consists of input and constants.
     * @param inputRowType physical type of the input row which only consists of input.
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createUnboundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputRowType,
            RowType inputRowType,
            int rowTimeIdx,
            boolean isRowsClause,
            ExecNodeConfig config,
            RelBuilder relBuilder) {
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        new boolean[aggCalls.size()],
                        false, // needRetraction
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                // ROWS unbounded over process function
                return new RowTimeRowsUnboundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            } else {
                // RANGE unbounded over process function
                return new RowTimeRangeUnboundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            }
        } else {
            return new ProcTimeUnboundedPrecedingFunction<>(
                     config.getStateRetentionTime(),
                     TableConfigUtils.getMaxIdleStateRetentionTime(config),
                     genAggsHandler,
                     flattenAccTypes);
        }
    }

    /**
     * Create an ProcessFunction for ROWS clause bounded OVER window to evaluate final aggregate
     * value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputType physical type of the input row which consists of input and constants.
     * @param inputType physical type of the input row which only consists of input.
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createBoundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputType,
            RowType inputType,
            int rowTimeIdx,
            boolean isRowsClause,
            long precedingOffset,
            ExecNodeConfig config,
            RelBuilder relBuilder) {

        boolean[] aggCallNeedRetractions = new boolean[aggCalls.size()];
        Arrays.fill(aggCallNeedRetractions, true);
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        aggCallNeedRetractions,
                        true, // needInputCount,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needRetract()
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                return new RowTimeRowsBoundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset,
                        rowTimeIdx);
            } else {
                return new RowTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset, rowTimeIdx);
            }
        } else {
            if (isRowsClause) {
                return new ProcTimeRowsBoundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset);
            } else {
                return new ProcTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset);
            }
        }
    }
}
