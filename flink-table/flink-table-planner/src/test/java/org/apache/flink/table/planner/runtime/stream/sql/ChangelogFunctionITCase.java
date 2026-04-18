/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** IT tests for the built-in TO_CHANGELOG and FROM_CHANGELOG process table functions. */
class ChangelogFunctionITCase extends StreamingTestBase {

    @Test
    void testFromChangelogFailOnInvalidOpCode() {
        final List<Row> data =
                Arrays.asList(
                        Row.of(1, "INSERT", "Alice"),
                        Row.of(2, "INSERT", "Bob"),
                        Row.of(1, "UNKNOWN", "Alice2"),
                        Row.of(2, "DELETE", "Bob"));

        final String dataId = TestValuesTableFactory.registerData(data);

        tEnv().executeSql(
                String.format(
                        "CREATE TABLE cdc_stream (\n"
                                + "  id INT,\n"
                                + "  op STRING,\n"
                                + "  name STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId));

        assertThatThrownBy(
                        () ->
                                CollectionUtil.iteratorToList(
                                        tEnv().executeSql(
                                                        "SELECT * FROM FROM_CHANGELOG("
                                                                + "input => TABLE cdc_stream, "
                                                                + "invalid_op_handling => 'FAIL')")
                                                .collect()))
                .rootCause()
                .hasMessageContaining("Received invalid op code 'UNKNOWN'");
    }
}
