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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test creates a scenario where bulk reader reads the most recently updated value with EACH_QUORUM
 * but reads stale value with QUORUM. This shows that QUORUM is different from EACH_QUORUM in multi-dc settings.
 * Here node5(DC2) and node6(DC2) has the update value for TEST_KEY.
 */
public class MultiDCEachQuorumIsNotQuorumTest extends BulkReaderMultiDCTestBase
{
    /**
     * Tests that QUORUM differs from EACH_QUORUM in multi-DC environments.
     *
     * @throws NoSuchMethodException if reflection fails
     */
    @Test
    void eachQuorumIsNotQuorum() throws NoSuchMethodException
    {
        List<String> updatedDataSet = new ArrayList<>(OG_DATASET);
        updatedDataSet.set(1, TEST_VAL);

        // Internally update value for TEST_KEY for node5 and node6. This update doesn't propagate to other nodes.
        updateValueNodeInternal(5, TEST_KEY, TEST_VAL);
        updateValueNodeInternal(6, TEST_KEY, TEST_VAL);

        // Bytecode injection to simulate a scenario where node5 and node6 are at the end of the replica list for bulk reader.
        // This simulation mimics a real world scenario.
        // With this arrangement PartitionedDataLayer.splitReplicas method for QUORUM will split the replicas like below:
        // primaryReplicas: [Node1, Node2, Node3, Node4]
        // secondaryReplicas: [Node5, Node6]
        // Number of nodes required for QUORUM read id 6/1 + 1 = 4. Bulk reader will read from [Node1, Node2, Node3, Node4] only.
        ByteBuddyAgent.install();
        new ByteBuddy()
        .redefine(CassandraDataLayer.class)
        .method(ElementMatchers.named("getAvailability"))
        .intercept(
        MethodCall.invoke(BulkReaderMultiDCTestBase.class.getMethod("getAvailability", CassandraInstance.class))
                  .withAllArguments()
        )
        .make()
        .load(
        CassandraDataLayer.class.getClassLoader(),
        ClassReloadingStrategy.fromInstalledAgent()
        );

        // Bulk read with QUORUM consistency
        List<Row> rowList = bulkRead(ConsistencyLevel.QUORUM.name());
        // Validate that the result doesn't have the updated data.
        validateBulkReadRows(rowList, OG_DATASET);

        // Message filter to mimic message drops from Node5 and Node6 to Node1.
        // We are setting this up to simulate a scenario where reading values with QUORUM consistency with driver
        // and using Node1 as the coordinator doesn't get the values from Node5 and Node6.
        cluster.filters().allVerbs().from(5).to(1).drop();
        cluster.filters().allVerbs().from(6).to(1).drop();

        // Read value for TEST_KEY with driver using Node1 as coordinator
        String quorumVal = readValueForKey(cluster.get(1).coordinator(), TEST_KEY, ConsistencyLevel.QUORUM);
        // Validate that the updated value is not read
        assertThat(quorumVal).isEqualTo(OG_DATASET.get(TEST_KEY));

        // Cleanup message filter
        cluster.filters().reset();

        // Bulk read with EACH_QUORUM consistency
        rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
        // Validate that bulk reader was able to read the updated value
        validateBulkReadRows(rowList, updatedDataSet);
        // Read value using driver with EACH_QUORUM
        String eachQuorumVal = readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
        // Validate that EACH_QUORUM read using driver and the bulk reader are the same
        assertThat(eachQuorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));

        // Revert the value update for all nodes
        setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
    }
}
