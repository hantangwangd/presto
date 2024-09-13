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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkIcebergQuery
{
    @Param({"300 * 3", "400 * 4"})
    private String recordCount = "300 * 3";
    DistributedQueryRunner queryRunner;
    @Setup
    public void setup() throws Exception
    {
        queryRunner = createIcebergQueryRunner(ImmutableMap.of(), Optional.empty());
        queryRunner.execute("create table iceberg_partition(a int, b int) with (partitioning = ARRAY['a', 'b'])");
        String[] batchAndPerBatch = recordCount.split("\\*");
        int batchCount = Integer.valueOf(batchAndPerBatch[0].trim());
        int coutPerBatch = Integer.valueOf(batchAndPerBatch[1].trim());
        for (int b = 0; b < batchCount; b++) {
            StringBuilder sqlBuilder = new StringBuilder("values(" + b + ", " + b + ")");
            for (int i = 1; i < coutPerBatch; i++) {
                sqlBuilder.append(String.format(", (%d, %d)", b, i));
            }
            String valuesSql = sqlBuilder.toString();
            queryRunner.execute("insert into iceberg_partition " + valuesSql);
        }
    }

    @Benchmark
    public void testFurtherOptimize(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute("select min(a), max(a), min(b), max(b) from iceberg_partition");
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void testOptimize(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute("select a, min(b), max(b) from iceberg_partition group by a");
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void testNormalQuery(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute("select a, b from iceberg_partition");
        bh.consume(result.getRowCount());
    }

    @TearDown
    public void finish()
    {
        queryRunner.execute("drop table iceberg_partition");
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkIcebergQuery.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
