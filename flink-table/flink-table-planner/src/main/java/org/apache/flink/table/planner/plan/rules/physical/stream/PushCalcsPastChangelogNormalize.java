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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Stack;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.planner.plan.utils.RexNodeExtractor.extractRefInputFields;

/**
 */
@Internal
public class PushCalcsPastChangelogNormalize {

    private static final Logger LOG = LoggerFactory.getLogger(PushCalcsPastChangelogNormalize.class);

    public PushCalcsPastChangelogNormalize() {
    }

    public static List<RelNode> optimize(RelBuilder builder, List<RelNode> inputs) {
        ArrayList<RelNode> newInputs = new ArrayList<RelNode>(inputs.size());
        for (RelNode input : inputs) {
            PushCalcsVisitor shuttle = new PushCalcsVisitor();
            LOG.info("optimize called with input node {}", input);
            shuttle.go(input);
            RelNode newInput = shuttle.transform(input, builder);
            newInputs.add(newInput);
        }
        return newInputs;
    }

    static class PushCalcsVisitor extends RelVisitor {
        private Stack<RelNode> stack = new Stack<RelNode>();
        private HashMap<StreamPhysicalTableSourceScan, boolean[]> usedColumnsBySource = new HashMap<>();
        private List<ArrayList<RelNode>> matches = new ArrayList<ArrayList<RelNode>>();

        public PushCalcsVisitor() {
            super();
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            stack.push(node);
            if (node instanceof StreamPhysicalTableSourceScan) {
                StreamPhysicalTableSourceScan source = (StreamPhysicalTableSourceScan) node;
                boolean[] usedColumns = findUsedColumns(source, stack);
                boolean[] cachedUsedColumns;
                if (usedColumnsBySource.containsKey(source)) {
                    cachedUsedColumns = usedColumnsBySource.get(source);
                } else {
                    cachedUsedColumns = new boolean[usedColumns.length];
                    usedColumnsBySource.put(source, cachedUsedColumns);
                }
                for (int i = 0; i < usedColumns.length; i++) {
                    if (usedColumns[i]) {
                        cachedUsedColumns[i] = true;
                    }
                }
            }
            super.visit(node, ordinal, parent);
            stack.pop();
        }

        private ArrayList<RelNode> pathMatches(Object[] matchConfig, Stack<RelNode> path) {
            if (path.size() < matchConfig.length) {
                return null;
            }

            ArrayList<RelNode> match = new ArrayList<RelNode>(matchConfig.length);
            path = (Stack<RelNode>) path.clone();
            RelNode node = null;
            for (int i = 0; i < matchConfig.length; i++) {
                node = path.pop();
                if (node.getClass() != matchConfig[i]) {
                    return null;
                }
                match.add(node);
            }

            return match;
        }

        private void computeUsedColumns(StreamPhysicalCalc calc, StreamPhysicalChangelogNormalize changelogNormalize, boolean[] usedColumns) {
            // Create a union list of fields between the projected columns and unique keys
            final RelDataType inputRowType = changelogNormalize.getRowType();

            // unique key indexes
            for (int pidx : changelogNormalize.uniqueKeys()) {
                usedColumns[pidx] = true;
            }

            final RexProgram program = calc.getProgram();

            // column references in the projection list
            for (RexLocalRef expr : program.getProjectList()) {
                // projections can be simple column identieties but they can also contain
                // expressions, so we need to have a more robust way of extracting all
                // column references from the projection list with the helper below
                for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(expr)))) {
                    usedColumns[ref] = true;
                }
            }

            // column references in any of the predicates
            RexLocalRef condition = program.getCondition();
            if (condition != null) {
                for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(condition)))) {
                    usedColumns[ref] = true;
                }
            }
        }

        /**
         * Walk up the physical plan tree to find a calc node right after ChangelogNormalize
         * which projects a subset of the columns used by the source scan. If we find such
         * calc nodes, then we can update our used columns map.
         */
        private boolean[] findUsedColumns(StreamPhysicalTableSourceScan source, final Stack<RelNode> stack) {
            Stack<RelNode> path = (Stack<RelNode>) stack.clone();
            RelDataType sourceRowType = source.getRowType();
            boolean[] usedColumns = new boolean[sourceRowType.getFieldCount()];

            final Object[] matchConfig = {
                StreamPhysicalTableSourceScan.class,
                StreamPhysicalDropUpdateBefore.class,
                StreamPhysicalExchange.class,
                StreamPhysicalChangelogNormalize.class,
                StreamPhysicalCalc.class
            };

            ArrayList<RelNode> match = pathMatches(matchConfig, path);
            if (match != null) {
                StreamPhysicalCalc calc = (StreamPhysicalCalc) match.get(match.size() - 1);
                StreamPhysicalChangelogNormalize changelogNormalize = (StreamPhysicalChangelogNormalize) match.get(match.size() - 2);
                computeUsedColumns(calc, changelogNormalize, usedColumns);
                matches.add(match);
            } else {
                // if we don't see a match to the plan, then assume all columns have been used
                // by the subplan, effectively turning off this optization for the source
                for (int i = 0; i < usedColumns.length; i++) {
                    usedColumns[i] = true;
                }
            }

            return usedColumns;
        }

        public RelNode transform(RelNode input, RelBuilder relBuilder) {
            for (ArrayList<RelNode> match : matches) {
                StreamPhysicalTableSourceScan source = (StreamPhysicalTableSourceScan) match.get(0);
                StreamPhysicalCalc calc = (StreamPhysicalCalc) match.get(match.size() - 1);
                boolean[] usedColumns = usedColumnsBySource.get(source);
                int numUsedColumns = 0;
                for (int i = 0; i < usedColumns.length; i++) {
                    if (usedColumns[i]) {
                        numUsedColumns++;
                    }
                }
                if (numUsedColumns == usedColumns.length) {
                    // all columns are used, do not apply transformation
                    LOG.info("SOURCE DO NOT APPLY source {}", source);
                } else {
                    LOG.info("SOURCE APPLY source {} calc {} usedColumn {}", source, calc, usedColumns);
                    RelNode newNode = transform(relBuilder, calc, usedColumns);
                    input = rewrite(input, calc, newNode);
                }
            }
            return input;
        }

        private RelNode rewrite(RelNode root, RelNode origNode, RelNode newNode) {
            if (origNode == newNode) {
                return root;
            }

            if (root == origNode) {
                return newNode;
            }

            RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    if (node == origNode) {
                        LOG.info("REPLACE node {} origNode {} parent {}", node, origNode, parent);
                        parent.replaceInput(0, newNode);
                    } else {
                        super.visit(node, ordinal, parent);
                    }
                }
            };

            visitor.go(root);

            return root; 
        }

 
        private RelNode transform(RelBuilder relBuilder, StreamPhysicalCalc calc, boolean[] usedColumns) {
            StreamPhysicalChangelogNormalize changelogNormalize = (StreamPhysicalChangelogNormalize) calc.getInput();
            // Create a union list of fields between the projected columns and unique keys
            final RelDataType inputRowType = changelogNormalize.getRowType();
            final int[] inputRemap = new int[inputRowType.getFieldCount()];

            final RexProgram program = calc.getProgram();

            // we need to know the new column index mappings for the calc node
            // so the array is going to be inputRemap[oldIndex] = new index (or -1 if not needed)
            for (int prev = 0, curr = 0; prev < usedColumns.length; prev++) {
                if (usedColumns[prev]) {
                    inputRemap[prev] = curr;
                    curr++;
                } else {
                    inputRemap[prev] = -1;
                }
            }

            // Construct a new ChangelogNormalize which has the new projection pushed into it
            StreamPhysicalChangelogNormalize newChangelogNormalize =
                    pushNeededColumnsThroughChangelogNormalize(relBuilder, changelogNormalize, usedColumns);

            StreamPhysicalCalc newCalc = projectCopyWithRemap(
                relBuilder, newChangelogNormalize, calc, inputRemap);

            if (newCalc.getProgram().isTrivial()) {
                return newChangelogNormalize;
            } else {
                return newCalc;
            }
        }

        private StreamPhysicalChangelogNormalize pushNeededColumnsThroughChangelogNormalize(
                RelBuilder relBuilder, StreamPhysicalChangelogNormalize changelogNormalize,
                boolean[] usedColumns) {
            final StreamPhysicalExchange exchange = (StreamPhysicalExchange) changelogNormalize.getInput();

            final StreamPhysicalCalc pushedProjectionCalc =
                    projectWithNeededColumns(
                            relBuilder, exchange.getInput(), usedColumns);

            final StreamPhysicalExchange newExchange =
                    (StreamPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(),
                                    Collections.singletonList(pushedProjectionCalc));

            return (StreamPhysicalChangelogNormalize)
                    changelogNormalize.copy(
                            changelogNormalize.getTraitSet(), Collections.singletonList(newExchange));
        }

        private StreamPhysicalCalc projectWithNeededColumns(
                RelBuilder relBuilder, RelNode newInput, boolean[] usedColumns) {

            final RexProgramBuilder programBuilder =
                    new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());
        
            for (RelDataTypeField field : newInput.getRowType().getFieldList()) {
                if (usedColumns[field.getIndex()]) {
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

        private StreamPhysicalCalc projectCopyWithRemap(RelBuilder relBuilder,
                    RelNode newInput, StreamPhysicalCalc origCalc, int[] inputRemap) {

            // We have the original calc which references inputs from the wider row
            // before we projected away the unused columns. In the code below, we need
            // to take all the references in the original Calc and remap that to the
            // new (reduced) input row.

            final RexProgramBuilder programBuilder =
                new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());

            RexShuttle remapVisitor = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return new RexInputRef(inputRemap[inputRef.getIndex()], inputRef.getType());
                }
            };

            // rewrite all the simple projections
            for (RexLocalRef ref : origCalc.getProgram().getProjectList()) {
                RexNode expandedRef  = origCalc.getProgram().expandLocalRef(ref);
                RexNode newRef = expandedRef.accept(remapVisitor);
                programBuilder.addProject(newRef, ref.getName());
            }

            // rewrite any predicates (if exists)
            RexLocalRef conditionLocalRef = origCalc.getProgram().getCondition();
            if (conditionLocalRef != null) {
                RexNode expandedRef  = origCalc.getProgram().expandLocalRef(conditionLocalRef);
                RexNode newRef = expandedRef.accept(remapVisitor);
                programBuilder.addCondition(newRef);
            }

            final RexProgram newProgram = programBuilder.getProgram();
            return new StreamPhysicalCalc(
                    newInput.getCluster(),
                    newInput.getTraitSet(),
                    newInput,
                    newProgram,
                    origCalc.getProgram().getOutputRowType());
        }
    }
}
