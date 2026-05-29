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

package org.apache.flink.table.planner.operations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for in-place conversion of a regular table to a materialized table via {@code CREATE OR
 * ALTER MATERIALIZED TABLE}.
 */
class SqlNodeToOperationConvertTableToMaterializedTableTest
        extends SqlNodeToOperationConversionTestBase {

    private static final String SOURCE_REGULAR_TABLE_NAME = "src_table";

    @BeforeEach
    void before() throws TableAlreadyExistException, DatabaseNotExistException {
        super.before();
        sourceTable(SOURCE_REGULAR_TABLE_NAME).create();
        sourceTable("t1_with_ts").create();
    }

    @Nested
    class OperationSelection {
        private static final String EXISTING_MT_NAME = "existing_mt";

        @Test
        void missingTargetCreatesMaterializedTable() {
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE brand_new"
                            + " FRESHNESS = INTERVAL '1' MINUTE"
                            + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(CreateMaterializedTableOperation.class);
        }

        @Test
        void existingMaterializedTableAlters()
                throws TableAlreadyExistException, DatabaseNotExistException {
            configureConversionEnabled(true);
            createExistingMaterializedTable();
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + EXISTING_MT_NAME
                            + " FRESHNESS = INTERVAL '1' MINUTE"
                            + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(FullAlterMaterializedTableOperation.class);
        }

        @Test
        void regularTableWithConversionDisabledIsRejected() {
            configureConversionEnabled(false);
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + SOURCE_REGULAR_TABLE_NAME
                            + " FRESHNESS = INTERVAL '1' MINUTE"
                            + " AS SELECT a, b FROM t1";
            assertThatThrownBy(() -> parse(sql))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("is not a materialized table");
        }

        @Test
        void regularTableWithConversionEnabledIsConverted() {
            configureConversionEnabled(true);
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + SOURCE_REGULAR_TABLE_NAME
                            + " FRESHNESS = INTERVAL '1' MINUTE"
                            + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        private void createExistingMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            final String sql =
                    "CREATE MATERIALIZED TABLE existing_mt (\n"
                            + "  CONSTRAINT pk1 PRIMARY KEY(a) NOT ENFORCED\n"
                            + ")\n"
                            + "FRESHNESS = INTERVAL '1' MINUTE\n"
                            + "AS SELECT a, b FROM t1";
            final Operation op = parse(sql);
            assertThat(op).isInstanceOf(CreateMaterializedTableOperation.class);
            final CatalogMaterializedTable mt =
                    ((CreateMaterializedTableOperation) op).getCatalogMaterializedTable();
            catalog.createTable(
                    new ObjectPath(catalogManager.getCurrentDatabase(), EXISTING_MT_NAME),
                    mt,
                    true);
        }
    }

    @Nested
    class ConfigScope {

        @Test
        void sessionOnlyEnableHasNoEffect() {
            tableConfig.set(
                    TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
            // root configuration left default (false)

            assertThatThrownBy(() -> parse(conversionSql()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("is not a materialized table");
        }

        @Test
        void clusterRootEnableAllowsConversion() {
            configureConversionEnabled(true);

            assertThat(parse(conversionSql()))
                    .isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        @Test
        void bothSessionAndClusterEnabledAllowsConversion() {
            tableConfig.set(
                    TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
            configureConversionEnabled(true);

            assertThat(parse(conversionSql()))
                    .isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        @Test
        void neitherSessionNorClusterEnabledIsRejected() {
            // nothing set
            assertThatThrownBy(() -> parse(conversionSql()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("is not a materialized table");
        }

        private String conversionSql() {
            return "CREATE OR ALTER MATERIALIZED TABLE "
                    + SOURCE_REGULAR_TABLE_NAME
                    + " FRESHNESS = INTERVAL '1' MINUTE"
                    + " AS SELECT a, b FROM t1";
        }
    }

    @Nested
    class WatermarkAndPrimaryKey {

        @BeforeEach
        void enableConversion() {
            configureConversionEnabled(true);
        }

        @Test
        void sourceWatermarkAndPrimaryKeyAreNotInherited()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_wm_pk").withWatermark().withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_wm_pk"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void columnListWithoutWatermarkOrPrimaryKeyDropsThem()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_list").withWatermark().withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_list ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3))"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void declaredWatermarkAndPrimaryKeyArePresent()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_neither").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_neither ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,"
                                    + " PRIMARY KEY (a) NOT ENFORCED)"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isNotEmpty();
            assertThat(newSchema.getPrimaryKey()).isPresent();
        }

        @Test
        void declaredWatermarkOnlyDropsSourcePrimaryKey()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_pk_only").withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_pk_only ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isNotEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void declaredPrimaryKeyOnlyDropsSourceWatermark()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_wm_only").withWatermark().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_wm_only ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " PRIMARY KEY (a) NOT ENFORCED)"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isPresent();
        }

        @Test
        void neitherSourceNorStatementDeclaresThem()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_plain").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_plain"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        private ResolvedSchema convertedMaterializedTableSchema(String sql) {
            return convertedOperation(sql).getMaterializedTable().getResolvedSchema();
        }
    }

    @Nested
    class QueryEvolution {

        @BeforeEach
        void enableConversion() {
            configureConversionEnabled(true);
        }

        @Test
        void queryAddsColumnNotInSourceAddsItToNewMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_add_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_add_col"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts, CAST('extra' AS STRING) AS c FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts", "c");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.AddColumn
                                            && "c"
                                                    .equals(
                                                            ((TableChange.AddColumn) change)
                                                                    .getColumn()
                                                                    .getName()));
        }

        @Test
        void queryDropsColumnFromSourceDropsItFromNewMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_drop_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_drop_col"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.DropColumn
                                            && "ts"
                                                    .equals(
                                                            ((TableChange.DropColumn) change)
                                                                    .getColumnName()));
        }

        @Test
        void queryRenamesColumnViaAliasIsModeledAsDropAndAdd()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_rename_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_rename_col"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT a, b, ts AS event_time FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "event_time");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.DropColumn
                                            && "ts"
                                                    .equals(
                                                            ((TableChange.DropColumn) change)
                                                                    .getColumnName()))
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.AddColumn
                                            && "event_time"
                                                    .equals(
                                                            ((TableChange.AddColumn) change)
                                                                    .getColumn()
                                                                    .getName()));
        }

        @Test
        void queryChangesColumnTypeIsModeledAsModifyPhysicalColumnType()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3)
            sourceTable("src_type_change").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_type_change"
                                    + " FRESHNESS = INTERVAL '1' MINUTE"
                                    + " AS SELECT CAST(a AS INT) AS a, b, ts FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumn("a"))
                    .hasValueSatisfying(
                            column ->
                                    assertThat(column.getDataType().getLogicalType().getTypeRoot())
                                            .isEqualTo(LogicalTypeRoot.INTEGER));
            assertThat(op.getTableChanges())
                    .anyMatch(change -> change instanceof TableChange.ModifyPhysicalColumnType);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private void configureConversionEnabled(boolean enabled) {
        final Configuration root = new Configuration();
        root.set(TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, enabled);
        tableConfig.setRootConfiguration(root);
    }

    private ConvertTableToMaterializedTableOperation convertedOperation(String sql) {
        final Operation op = parse(sql);
        assertThat(op).isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        return (ConvertTableToMaterializedTableOperation) op;
    }

    private SourceTableBuilder sourceTable(String name) {
        return new SourceTableBuilder(name);
    }

    /** Fluent builder for registering a regular {@code (a, b, ts)} source table in the catalog. */
    private final class SourceTableBuilder {

        private final String name;
        private boolean withWatermark;
        private boolean withPrimaryKey;

        private SourceTableBuilder(String name) {
            this.name = name;
        }

        SourceTableBuilder withWatermark() {
            this.withWatermark = true;
            return this;
        }

        SourceTableBuilder withPrimaryKey() {
            this.withPrimaryKey = true;
            return this;
        }

        void create() throws TableAlreadyExistException, DatabaseNotExistException {
            final Schema.Builder schema = Schema.newBuilder();
            schema.column("a", DataTypes.BIGINT().notNull());
            schema.column("b", DataTypes.STRING());
            schema.column("ts", DataTypes.TIMESTAMP(3));
            if (withWatermark) {
                schema.watermark("ts", new SqlCallExpression("ts - INTERVAL '5' SECOND"));
            }
            if (withPrimaryKey) {
                schema.primaryKeyNamed("pk_src", List.of("a"));
            }
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "COLLECTION");
            final CatalogTable table =
                    CatalogTable.newBuilder().schema(schema.build()).options(options).build();
            catalog.createTable(
                    new ObjectPath(catalogManager.getCurrentDatabase(), name), table, false);
        }
    }
}
