/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.analytics.bulkreadermultidc;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that:
 * QUORUM read succeeds with two nodes down in a single DC.
 * QUORUM read value using bulk reader equals QUORUM read value using driver.
 * EACH_QUORUM read with bulk reader fails with cause as NotEnoughReplicasException.
 * EACH_QUORUM read with driver fails.
 */
public class MultiDCEachQuorumFailureTest extends BulkReaderMultiDCTestBase
{
    /**
     * Tests EACH_QUORUM failure when two nodes are down in one datacenter.
     *
     * @throws Exception if cluster operations fail
     */
    @Test
    void eachQuorumFailureWithTwoNodesDownOneDC() throws Exception
    {
        // Stop Node4(DC2)
        cluster.stopUnchecked(cluster.get(4));
        // Stop Node5(DC2)
        cluster.stopUnchecked(cluster.get(5));

        // Bulk read with QUORUM
        List<Row> rowList = bulkRead(ConsistencyLevel.QUORUM.name());
        validateBulkReadRows(rowList, OG_DATASET);
        // Driver read with QUORUM
        String quorumVal = readValueForKey(TEST_KEY, ConsistencyLevel.QUORUM);
        // Bulk read and driver read values are the same
        assertThat(quorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));

        // Try bulk reading with EACH_QUORUM consistency. Assert that it fails with the correct cause.
        try
        {
            bulkRead(ConsistencyLevel.EACH_QUORUM.name());
        }
        catch (Exception ex)
        {
            assertThat(ex).isNotNull();
            assertThat(ex).isInstanceOf(SparkException.class);
            assertThat(ex.getCause()).isInstanceOf(NotEnoughReplicasException.class);
            assertThat(ex.getCause().getMessage()).isEqualTo("Required 2 replicas but only 1 responded");
        }

        // Try driver reading with EACH_QUORUM consistency. Assert that it fails with the correct error.
        try
        {
            readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
        }
        catch (Exception ex)
        {
            assertThat(ex).isNotNull();
            assertThat(ex.getMessage()).isEqualTo("Cannot achieve consistency level EACH_QUORUM in DC datacenter2");
        }
    }
}
