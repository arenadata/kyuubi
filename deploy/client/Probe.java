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
