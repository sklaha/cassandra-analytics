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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for BulkReader multi-DC consistency tests.
 * Contains shared setup, configuration, and helper methods.
 */
public abstract class BulkReaderMultiDCTestBase extends SharedClusterSparkIntegrationTestBase
{
    protected static final List<String> OG_DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    protected static final int TEST_KEY = 1;
    protected static final String TEST_VAL = "C*";
    protected QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .dcCount(2)
                    .nodesPerDc(3)
                    .dcAndRackSupplier((nodeId) -> {
                        switch (nodeId)
                        {
                            case 1:
                            case 2:
                            case 3:
                                return dcAndRack("datacenter1", "rack1");
                            case 4:
                            case 5:
                            case 6:
                                return dcAndRack("datacenter2", "rack1");
                            default:
                                return dcAndRack("", "");
                        }
                    });
    }

    public static PartitionedDataLayer.AvailabilityHint getAvailability(CassandraInstance instance)
    {
        if (instance.nodeName().equals("localhost5") || instance.nodeName().equals("localhost6"))
        {
            return PartitionedDataLayer.AvailabilityHint.MOVING;
        }
        return PartitionedDataLayer.AvailabilityHint.UP;
    }

    @NotNull
    protected List<Row> bulkRead(String consistency)
    {
        List<Row> rowList;
        Dataset<Row> dataForTable1;
        dataForTable1 = bulkReaderDataFrame(table1)
                        .option("consistencyLevel", consistency)
                        .option("dc", null)
                        .option("maxRetries", 1)
                        .option("maxMillisToSleep", 50)
                        .option("defaultMillisToSleep", 50)
                        .load();

        rowList = dataForTable1.collectAsList().stream()
                               .sorted(Comparator.comparing(row -> row.getInt(0)))
                               .collect(Collectors.toList());
        return rowList;
    }

    protected static void validateBulkReadRows(List<Row> rowList, List<String> dataSet)
    {
        for (int i = 0; i < dataSet.size(); i++)
        {
            assertThat(rowList.get(i).getInt(0)).isEqualTo(i);
            assertThat(rowList.get(i).getString(1)).isEqualTo(dataSet.get(i));
        }
    }

    protected void validateReadRepairIsDisabled()
    {
        // Update value for Node1 only
        updateValueNodeInternal(1, TEST_KEY, TEST_VAL);
        // Validate only Node1 has the updated value
        validateNodeInternalValue(1, TEST_KEY, TEST_VAL);
        validateNodeInternalValue(2, TEST_KEY, OG_DATASET.get(1));
        validateNodeInternalValue(5, TEST_KEY, OG_DATASET.get(1));

        // Read with ALL consistency using coordinator.
        // If read repair is enabled this should update the value for all nodes.
        readValueForKey(TEST_KEY, ConsistencyLevel.ALL);

        // Validate only Node1 has the updated value
        validateNodeInternalValue(1, TEST_KEY, TEST_VAL);
        validateNodeInternalValue(2, TEST_KEY, OG_DATASET.get(1));
        validateNodeInternalValue(5, TEST_KEY, OG_DATASET.get(1));

        // Revert the value update for all nodes
        setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        // Read repair disabled: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/read_repair.html?utm_source=chatgpt.com#none
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text) with read_repair='NONE';");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < OG_DATASET.size(); i++)
        {
            String value = OG_DATASET.get(i);
            String query1 = String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');", table1, i, value);

            firstRunningInstance.coordinator().execute(query1, ConsistencyLevel.ALL);
        }
        validateReadRepairIsDisabled();
    }

    protected void updateValueNodeInternal(int node, int key, String value)
    {
        cluster.get(node).executeInternal(String.format("UPDATE %s SET name='%s' WHERE id=%d", table1, value, key));
    }

    protected void validateNodeInternalValue(int node, int key, String val)
    {
        assertThat(getNodeInternalValue(node, key)).isEqualTo(val);
    }

    protected String getNodeInternalValue(int node, int key)
    {
        Object[][] result = cluster.get(node)
                                   .executeInternal(String.format("SELECT name FROM %s WHERE id=%d", table1, key));
        return (String) result[0][0];
    }

    protected String readValueForKey(int key, ConsistencyLevel consistencyLevel)
    {
        return readValueForKey(cluster.getFirstRunningInstance().coordinator(), key, consistencyLevel);
    }

    protected String readValueForKey(ICoordinator coordinator, int key, ConsistencyLevel consistencyLevel)
    {
        Object[][] result = coordinator
                            .execute(String.format("SELECT name FROM %s WHERE id=%d", table1, key), consistencyLevel);
        return (String) result[0][0];
    }

    /**
     * Sets value for a key with consistency level ALL.
     *
     * @param key   the key to update
     * @param value the value to set
     */
    protected void setValueForALL(int key, String value)
    {
        cluster.getFirstRunningInstance()
               .coordinator()
               .execute(String.format("UPDATE %s SET name='%s' WHERE id=%d", table1, value, key), ConsistencyLevel.ALL);
    }
}
