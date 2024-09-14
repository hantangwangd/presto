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
package com.facebook.presto.hive;

import com.facebook.presto.spi.LazyIterable;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

class LazyPartitionsIterable
        implements LazyIterable<HivePartition>
{
    private final LazyLoadedPartitions partitions;

    public LazyPartitionsIterable(LazyLoadedPartitions partitions)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @Override
    public Iterator<HivePartition> iterator()
    {
        return new LazyIterator(partitions);
    }

    @Override
    public void setMaxIterableCount(int maxIterableCount)
    {
        partitions.setMaxPartitionThreshold(maxIterableCount);
    }

    private static class LazyIterator
            extends AbstractIterator<HivePartition>
    {
        private final LazyLoadedPartitions lazyPartitions;
        private List<HivePartition> partitions;
        private int position = -1;

        private LazyIterator(LazyLoadedPartitions lazyPartitions)
        {
            this.lazyPartitions = lazyPartitions;
        }

        @Override
        protected HivePartition computeNext()
        {
            if (partitions == null) {
                partitions = lazyPartitions.getFullyLoadedPartitions();
            }

            position++;
            if (position >= partitions.size()) {
                return endOfData();
            }
            return partitions.get(position);
        }
    }
}
