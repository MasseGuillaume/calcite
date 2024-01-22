/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

/**
 * Tests for {@link RelToSqlConverter} on a schema that has nested structures of multiple
 * levels.
 */
class RelToSqlConverterStructsTest {

  private RelToSqlConverterTest.Sql sql(String sql) {
    return new RelToSqlConverterTest.Sql(CalciteAssert.SchemaSpec.MY_DB, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  @Test void testNestedSchemaSelectStar() {
    String query = "SELECT * FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "ROW(ROW(\"n1\".\"n11\".\"b\"), ROW(\"n1\".\"n12\".\"c\")) AS \"n1\", "
        + "ROW(\"n2\".\"d\") AS \"n2\", "
        + "\"xs\", \"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  @Test void testNestedSchemaRootColumns() {
    String query = "SELECT \"a\", \"e\" FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "\"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  @Test void testNestedSchemaNestedColumns() {
    String query = "SELECT \"a\", \"e\", "
        + "\"myTable\".\"n1\".\"n11\".\"b\", "
        + "\"myTable\".\"n2\".\"d\" "
        + "FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "\"e\", "
        + "\"n1\".\"n11\".\"b\", "
        + "\"n2\".\"d\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  @Test void testUncollectLateralJoin() {
    final String query = "select \"a\",\n"
        + "\"x\"\n"
        + "from \"myDb\".\"myTable\",\n"
        + "unnest(\"xs\") as \"x\"";

    /*
    query logical plan:

    LogicalProject(a=[$0], x=[$6])
      LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{4}])
        LogicalProject(a=[$0], b=[$1.n11.b], c=[$1.n12.c], d=[$2.d], xs=[$3], e=[$4])
          LogicalTableScan(table=[[myDb, myTable]])
        Uncollect
          LogicalProject(xs=[$cor0.xs])
            LogicalValues(tuples=[[{ 0 }]])

    obtained:

    SELECT 
      "$cor0"."a", 
      "$cor0"."xs0" AS "x"
    FROM (
      SELECT "a", "n1"."n11"."b", "n1"."n12"."c", "n2"."d", "xs", "e" FROM "myDb"."myTable"
    ) AS "$cor0",
    LATERAL UNNEST (
      SELECT "$cor0"."xs" FROM (VALUES (0)) AS "t" ("ZERO")
    ) AS "t1" ("xs") AS "t10"
    */

    final String expected = "SELECT 1";
    sql(query).schema(CalciteAssert.SchemaSpec.MY_DB).ok(expected);
  }
}
