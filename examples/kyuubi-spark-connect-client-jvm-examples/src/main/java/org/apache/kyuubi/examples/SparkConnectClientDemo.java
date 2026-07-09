/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kyuubi.KyuubiAuthType;
import org.apache.spark.sql.kyuubi.KyuubiSessionBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Kyuubi Spark Connect demo.
 *
 * Environment variables:
 *   KYUUBI_URL      - required, e.g.: sc://host:10199/;use_ssl=true
 *   KYUUBI_AUTH     - optional, one of: NONE, KERBEROS, LDAP (default: NONE)
 *   KYUUBI_USERNAME - required for LDAP
 *   KYUUBI_PASSWORD - required for LDAP
 *
 * Examples:
 *   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=KERBEROS java -jar app.jar
 *   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=LDAP KYUUBI_USERNAME=john KYUUBI_PASSWORD=secret java -jar app.jar
 */
public class SparkConnectClientDemo {

  public static void main(String[] args) throws InterruptedException {
    String url = System.getenv("KYUUBI_URL");
    if (url == null || url.isEmpty()) {
      throw new IllegalArgumentException("KYUUBI_URL environment variable is required");
    }

    String authStr = System.getenv().getOrDefault("KYUUBI_AUTH", "NONE");
    KyuubiAuthType authType = KyuubiAuthType.valueOf(authStr.toUpperCase());
    // KyuubiAuthType authType = KyuubiAuthType.kerberos();

    String username = System.getenv("KYUUBI_USERNAME");
    String password = System.getenv("KYUUBI_PASSWORD");

    System.out.println("connection string: " + url);
    System.out.println("auth type: " + authType);

    SparkSession spark = new KyuubiSessionBuilder(url, authType, username, password).getOrCreate();
    try {
      spark.sql("SELECT current_user()").show();

      StructType schema = DataTypes.createStructType(new StructField[]{
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("name", DataTypes.StringType, false),
          DataTypes.createStructField("age", DataTypes.IntegerType, false),
          DataTypes.createStructField("country", DataTypes.StringType, false)
      });
      List<Row> rows = Arrays.asList(
          RowFactory.create(1, "Ivan", 20, "Russia"),
          RowFactory.create(2, "Yao", 23, "China"),
          RowFactory.create(3, "Ann", 28, "Russia"),
          RowFactory.create(4, "John", 20, "USA")
      );
      Dataset<Row> df = spark.createDataFrame(rows, schema);
      df.printSchema();
      df.show();

      System.out.println("Sleeping 10 seconds");
      Thread.sleep(10_000);

      df.createOrReplaceTempView("people");

      for (int i = 0; i < 100; i++) {
        String sqlQuery = "SELECT * FROM people";
        System.out.println(sqlQuery);
        spark.sql(sqlQuery).show();
        System.out.println("i = " + i + ", sleeping 1s");
        Thread.sleep(1000);
      }
    } finally {
      spark.stop();
    }
  }
}
