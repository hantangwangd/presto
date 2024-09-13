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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LazyLoadedPartitions
{
    private boolean fullyLoaded;
    private PartitionLoader partitionLoader;
    private List<HivePartition> partitions;

    public LazyLoadedPartitions(List<HivePartition> partitions)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.fullyLoaded = true;
    }

    public LazyLoadedPartitions(PartitionLoader partitionLoader)
    {
        this.partitionLoader = requireNonNull(partitionLoader, "partitionLoader is null");
    }

    public List<HivePartition> getFullyLoadedPartitions()
    {
        tryFullyLoad();
        return this.partitions;
    }

    /**
     * This method may return an iterable with lazy loading
     * */
    public Iterable<HivePartition> getPartitionsIterable()
    {
        return new LazyPartitionsIterable(this);
    }

    public boolean isEmpty()
    {
        if (this.fullyLoaded) {
            return this.partitions.isEmpty();
        }
        else {
            return this.partitionLoader.isEmpty();
        }
    }

    private void tryFullyLoad()
    {
        if (!this.fullyLoaded) {
            synchronized (this) {
                if (!this.fullyLoaded) {
                    this.partitions = this.partitionLoader.loadPartitions();
                    this.fullyLoaded = true;
                    this.partitionLoader = null;
                }
            }
        }
    }

    public interface PartitionLoader
    {
        List<HivePartition> loadPartitions();

        boolean isEmpty();
    }
}
