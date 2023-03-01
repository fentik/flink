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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.fun.SqlRandFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 */
public class FlinkJoinSaltNullsRule extends RelRule<FlinkJoinSaltNullsRule.Config>
        implements TransformationRule {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJoinSaltNullsRule.class);

    public static final FlinkJoinSaltNullsRule INSTANCE =
            FlinkJoinSaltNullsRule.Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public FlinkJoinSaltNullsRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinSaltNullsRule(Class<? extends Join> clazz) {
        this(Config.DEFAULT.withOperandFor(clazz));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinSaltNullsRule(
            Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class)
                        .withOperandFor(joinClass));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final JoinInfo joinInfo = join.analyzeCondition();

        if (join.getJoinType() == JoinRelType.LEFT && joinInfo.isEqui()) {
            // Look for left joins with an equijoin condition
            return true;
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        final RelNode origLeft = call.rel(1);
        final RelNode origRight = call.rel(2);
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
        final RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);


        List<String> leftFieldNames = new ArrayList<>(origLeft.getRowType().getFieldNames());
        leftFieldNames.add("__rubisalt_left");

        RexNode leftSaltExpr = relBuilder.call(FlinkSqlOperatorTable.RAND, ImmutableList.of());

        RelNode leftSaltedProject = 
            relBuilder
                .push(origLeft)
                .project(Iterables.concat(relBuilder.fields(), ImmutableList.of(leftSaltExpr)), leftFieldNames, true)
                .build();

        LOG.info("SERGEI leftSaltedProject {} types {}", leftSaltedProject, leftSaltedProject.getRowType());


        List<String> rightFieldNames = new ArrayList<>(origRight.getRowType().getFieldNames());
        rightFieldNames.add("__rubisalt_right");

        RexNode rightSaltExpr = relBuilder.call(FlinkSqlOperatorTable.RAND, ImmutableList.of());

        RelNode rightSaltedProject = 
            relBuilder
                .push(origRight)
                .project(Iterables.concat(relBuilder.fields(), ImmutableList.of(rightSaltExpr)), rightFieldNames, true)
                .build();

        RexNode origJoinCondition = origJoin.getCondition();

        LOG.info("SERGEI rightSaltedProject {}", rightSaltedProject);

        RexNode saltyJoinCondition =
            relBuilder
                .push(leftSaltedProject)
                .push(rightSaltedProject)
                .and(origJoinCondition, relBuilder.equals(relBuilder.field(1), relBuilder.field(2)));

        final Join saltyJoin = origJoin.copy(
            origJoin.getTraitSet(),
            saltyJoinCondition,
            leftSaltedProject,
            rightSaltedProject,
            origJoin.getJoinType(),
            false);

        LOG.info("SERGEI salted join {} row type {}", saltyJoin, saltyJoin.getRowType());

        final RelNode saltyProject =
            relBuilder
                .push(saltyJoin)
                .projectExcept(relBuilder.fields(ImmutableList.of("__rubisalt_left", "__rubisalt_right")))
                .build();
            
        LOG.info("SERGEI salted project {}", saltyProject);

        // RexNode cond = origJoin.getCondition();
        // cond.accept(new RexShuttle() {
        //     public RexNode visitInputRef(RexInputRef node) {
        //         LOG.info("SERGEI cond visitor innput ref {}", node);
        //         return node;
        //     }
        // });

        call.transformTo(saltyProject);
        // call.transformTo(saltyJoin);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY.as(Config.class).withOperandFor(LogicalJoin.class);

        @Override
        default FlinkJoinSaltNullsRule toRule() {
            return new FlinkJoinSaltNullsRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(joinClass)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 -> b2.operand(RelNode.class).anyInputs()))
                    .as(Config.class);
        }
    }
}
