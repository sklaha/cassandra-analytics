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

import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.spark.sql.Row;

/**
 * Local reproduction tests for multi-DC consistency scenarios.
 */
public class LocalReproTest extends BulkReaderMultiDCTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalReproTest.class);

    @RepeatedTest(100)
    void localReproFail() throws InterruptedException
    {

        List<Row> rowList = bulkRead(ConsistencyLevel.QUORUM.name());
        LOGGER.error(String.valueOf(rowList.size()));

        cluster.filters().allVerbs().from(1).to(5).drop();
        cluster.filters().allVerbs().from(1).to(6).drop();

        // Read value for TEST_KEY with driver using Node1 as coordinator
        String quorumVal = readValueForKey(cluster.get(1).coordinator(), TEST_KEY, ConsistencyLevel.QUORUM);
        LOGGER.error(quorumVal);
    }

    @RepeatedTest(100)
    void localReproSuccess() throws InterruptedException
    {
        cluster.filters().allVerbs().from(1).to(5).drop();
        cluster.filters().allVerbs().from(1).to(6).drop();

        String quorumVal = readValueForKey(cluster.get(1).coordinator(), TEST_KEY, ConsistencyLevel.QUORUM);
        LOGGER.error(quorumVal);
    }
}
