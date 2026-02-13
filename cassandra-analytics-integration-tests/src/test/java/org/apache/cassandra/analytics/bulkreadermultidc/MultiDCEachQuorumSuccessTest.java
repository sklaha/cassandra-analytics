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
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that EACH_QUORUM read succeeds with one node down in each DC.
 * Tests that value read using driver is the same as the value read using bulk reader.
 */
public class MultiDCEachQuorumSuccessTest extends BulkReaderMultiDCTestBase
{
    /**
     * Tests EACH_QUORUM read success with one node down per datacenter.
     *
     * @throws Exception if cluster operations fail
     */
    @Test
    void eachQuorumSuccessWithOneNodeDownEachDC() throws Exception
    {
        // Stop Node1(DC1)
        cluster.stopUnchecked(cluster.get(1));
        // Stop Node4(DC2)
        cluster.stopUnchecked(cluster.get(4));

        // Bulk read with EACH_QUORUM consistency
        List<Row> rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
        validateBulkReadRows(rowList, OG_DATASET);

        // Read TEST_KEY using driver
        String eachQuorumVal = readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
        // Validate that data from driver and bulk reader are the same
        assertThat(eachQuorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));
    }
}
