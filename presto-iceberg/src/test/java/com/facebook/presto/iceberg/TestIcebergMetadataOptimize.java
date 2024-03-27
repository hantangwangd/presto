/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergSessionProperties.FURTHER_FLAG;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestIcebergMetadataOptimize
        extends AbstractTestQueryFramework
{
    protected TestIcebergMetadataOptimize() {}

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of(),
                new IcebergConfig().getFileFormat(), false, Optional.empty());
    }

    @Test
    public void testMetadataQueryOptimizerWithFurtherFlag()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = sessionWithFurtherFlag(true);
        try {
            queryRunner.execute("create table iceberg_partition_with_further_flag(a int, b int) with (partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into iceberg_partition_with_further_flag values(1, 1), (1, 2), (2, 3), (2, 4)");

            assertQuery(session, "select min(a), max(a), min(b), max(b) from iceberg_partition_with_further_flag",
                    "values(1, 2, 1, 4)");
            assertPlan(session, "select min(a), max(a), min(b), max(b) from iceberg_partition_with_further_flag",
                    output(strictProject(
                            ImmutableMap.of(
                                    "min(a)", expression("1"),
                                    "max(a)", expression("2"),
                                    "min(b)", expression("1"),
                                    "max(b)", expression("4")),
                            exchange(LOCAL, REPARTITION, values()))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS iceberg_partition_with_further_flag");
        }
    }

    @Test
    public void testMetadataQueryOptimizerWithoutFurtherFlag()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = sessionWithFurtherFlag(false);
        try {
            queryRunner.execute("create table iceberg_partition_without_further_flag(a int, b int) with (partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into iceberg_partition_without_further_flag values(1, 1), (1, 2), (2, 3), (2, 4)");

            assertQuery(session, "select min(a), max(a), min(b), max(b) from iceberg_partition_without_further_flag",
                    "values(1, 2, 1, 4)");
            assertPlan(session, "select min(a), max(a), min(b), max(b) from iceberg_partition_without_further_flag",
                    anyTree(aggregation(ImmutableMap.of("min(a)", functionCall("min", ImmutableList.of("a")),
                                    "max(a)", functionCall("max", ImmutableList.of("a")),
                                    "min(b)", functionCall("min", ImmutableList.of("b")),
                                    "max(b)", functionCall("max", ImmutableList.of("b"))),
                            SINGLE,
                            values(ImmutableList.of("a", "b"),
                                    ImmutableList.of(
                                            ImmutableList.of(new LongLiteral("1"), new LongLiteral("1")),
                                            ImmutableList.of(new LongLiteral("1"), new LongLiteral("2")),
                                            ImmutableList.of(new LongLiteral("2"), new LongLiteral("3")),
                                            ImmutableList.of(new LongLiteral("2"), new LongLiteral("4")))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS iceberg_partition_without_further_flag");
        }
    }

    private Session sessionWithFurtherFlag(boolean furtherFlag)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, FURTHER_FLAG, furtherFlag ? "true" : "false")
                .build();
    }
}
