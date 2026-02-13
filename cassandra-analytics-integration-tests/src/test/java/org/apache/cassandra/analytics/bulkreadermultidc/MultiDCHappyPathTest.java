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

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Happy path test. All nodes have the updated values.
 * QUORUM == EACH_QUORUM == ALL == driver read
 */
public class MultiDCHappyPathTest extends BulkReaderMultiDCTestBase
{
    @Test
    void happyPathTest()
    {
        List<String> testDataSet = new ArrayList<>(OG_DATASET);
        testDataSet.set(TEST_KEY, TEST_VAL);

        // Set value=TEST_VAL for key=TEST_KEY for all nodes
        setValueForALL(TEST_KEY, TEST_VAL);

        // Bulk read with ALL consistency
        List<Row> rowList = bulkRead(ConsistencyLevel.ALL.name());
        validateBulkReadRows(rowList, testDataSet);

        // Bulk read with QUORUM consistency
        rowList = bulkRead(ConsistencyLevel.QUORUM.name());
        validateBulkReadRows(rowList, testDataSet);

        // Bulk read with EACH_QUORUM consistency
        rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
        validateBulkReadRows(rowList, testDataSet);

        // Read the value for the test key using driver for different consistency levels
        String valAll = readValueForKey(TEST_KEY, ConsistencyLevel.ALL);
        String valQuorum = readValueForKey(TEST_KEY, ConsistencyLevel.QUORUM);
        String valEachQuorum = readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
        assertThat(valAll).isEqualTo(valQuorum).isEqualTo(valEachQuorum).isEqualTo(rowList.get(1).getString(1));

        // Revert the value update for all nodes
        setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
    }
}
