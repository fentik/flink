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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.planner.plan.utils.RexNodeExtractor.extractRefInputFields;

/**
 */
@Internal
public class PushCalcsPastChangelogNormalizeRule
        extends RelRule<PushCalcsPastChangelogNormalizeRule.Config> {

    private static final Logger LOG = LoggerFactory.getLogger(PushCalcsPastChangelogNormalizeRule.class);
    public static final RelOptRule INSTANCE = Config.EMPTY.as(Config.class).onMatch().toRule();

    boolean fired = false;

    public PushCalcsPastChangelogNormalizeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalCalc calc = call.rel(0);
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);

        if (fired) {
            LOG.info("rule already fired, return");
            call.transformTo(calc);
            return;
        }
        LOG.info("RULE matched uniqueKeys() {} input {}", changelogNormalize.uniqueKeys(), changelogNormalize.getRowType());

        // Create a union list of fields between the projected columns and unique keys
        final RelDataType inputRowType = changelogNormalize.getRowType();
        final boolean[] isColumnNeeded = new boolean[inputRowType.getFieldCount()];

        // unique key indexes
        for (int pidx : changelogNormalize.uniqueKeys()) {
            isColumnNeeded[pidx] = true;
        }

        final RexProgram program = calc.getProgram();

        // column references in the projection list
        for (RexLocalRef expr : program.getProjectList()) {
            // projections can be simple column identieties but they can also contain
            // expressions, so we need to have a more robust way of extracting all
            // column references from the projection list with the helper below
            for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(expr)))) {
                isColumnNeeded[ref] = true;
            }
        }

        // column references in any of the predicates
        RexLocalRef condition = program.getCondition();
        if (condition != null) {
            for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(condition)))) {
                isColumnNeeded[ref] = true;
            }
        }

        boolean allColumnsNeeded = true;
        for (boolean isNeeded : isColumnNeeded) {
            if (isNeeded == false) {
                allColumnsNeeded = false;
                break;
            }
        }

        if (allColumnsNeeded) {
            // all columns are needed, no need to push a new projection
            call.transformTo(calc);
            return;
        }


        fired = true;


        // Construct a new ChangelogNormalize which has the new projection pushed into it
        StreamPhysicalChangelogNormalize newChangelogNormalize =
                pushNeededColumnsThroughChangelogNormalize(call, isColumnNeeded);

        StreamPhysicalCalc newCalc = (StreamPhysicalCalc)
            calc.copy(newChangelogNormalize.getTraitSet(), Collections.singletonList(newChangelogNormalize));

        LOG.info("newChangelogNormalize {}", newChangelogNormalize);
        LOG.info("newCalc {}", newCalc);
        call.transformTo(newCalc);
    }

    private StreamPhysicalChangelogNormalize pushNeededColumnsThroughChangelogNormalize(
            RelOptRuleCall call, boolean[] isColumnNeeded) {
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
        final StreamPhysicalExchange exchange = call.rel(2);

 
        final StreamPhysicalCalc pushedProjectionCalc =
                projectWithNeededColumns(
                        call.builder(), exchange.getInput(), isColumnNeeded);

        LOG.info("pushedProjectionCalc {}", pushedProjectionCalc);

        final StreamPhysicalExchange newExchange =
                (StreamPhysicalExchange)
                        exchange.copy(
                                exchange.getTraitSet(),
                                Collections.singletonList(pushedProjectionCalc));

        LOG.info("newExchnage {}", newExchange);

        return (StreamPhysicalChangelogNormalize)
                changelogNormalize.copy(
                        changelogNormalize.getTraitSet(), Collections.singletonList(newExchange));
    }

    private StreamPhysicalCalc projectWithNeededColumns(
            RelBuilder relBuilder, RelNode newInput, boolean[] isColumnNeeded) {

        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());
       
        for (RelDataTypeField field : newInput.getRowType().getFieldList()) {
            if (isColumnNeeded[field.getIndex()]) {
                programBuilder.addProject(new RexInputRef(field.getIndex(), field.getType()), field.getName());
            }
        }

        final RexProgram newProgram = programBuilder.getProgram();
        return new StreamPhysicalCalc(
                newInput.getCluster(),
                newInput.getTraitSet(),
                newInput,
                newProgram,
                newProgram.getOutputRowType());
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link PushCalcsPastChangelogNormalizeRule}. */
    public interface Config extends RelRule.Config {

        @Override
        default RelOptRule toRule() {
            return new PushCalcsPastChangelogNormalizeRule(this);
        }

        default Config onMatch() {
            final RelRule.OperandTransform exchangeTransform =
                    operandBuilder ->
                            operandBuilder.operand(StreamPhysicalExchange.class).anyInputs();

            final RelRule.OperandTransform changelogNormalizeTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalChangelogNormalize.class)
                                    .oneInput(exchangeTransform);

            final RelRule.OperandTransform calcTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalCalc.class)
                                    .oneInput(changelogNormalizeTransform);

            return withOperandSupplier(calcTransform).as(Config.class);
        }
    }
}
