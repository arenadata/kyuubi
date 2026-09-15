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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Opens one session against the gateway and reports what came back.
 *
 * The ticket is acquired by the JVM itself from a keytab through the JAAS entry
 * named in java.security.auth.login.config, so this needs no kinit and no krb5
 * tools - which is what lets the client image stay a bare JRE.
 */
public class Probe {
  public static void main(String[] args) throws Exception {
    String url = args[0];
    Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver");
    try (Connection c = DriverManager.getConnection(url)) {
      DatabaseMetaData md = c.getMetaData();
      // Not getUserName(): the driver does not implement it, and a probe that
      // failed on an unsupported call would report a working connection as a
      // refusal. The identity that arrived is in the gateway's log, which is
      // the only place it can be observed anyway.
      System.out.println("OK server=" + md.getDatabaseProductName()
          + " driver=" + md.getDriverName());
    } catch (SQLException e) {
      System.out.println("REFUSED " + e.getMessage().split("\n")[0]);
      System.exit(1);
    }
  }
}
