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

package org.apache.kyuubi.it.yarn

import java.io.{BufferedWriter, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration
import java.util.concurrent.Future
import java.util.function.Consumer

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.github.dockerjava.api.command.CreateContainerCmd
import org.testcontainers.containers.{BindMode, GenericContainer}
import org.testcontainers.containers.Container.ExecResult
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.RemoteDockerImage
import org.testcontainers.images.builder.{ImageFromDockerfile, Transferable}
import org.testcontainers.utility.{DockerImageName, MountableFile}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.it.yarn.KerberizedYarnCluster._

/**
 * A kerberized single node Hadoop cluster in Docker containers: an MIT Kerberos KDC, ZooKeeper,
 * an HDFS NameNode and DataNode, a YARN ResourceManager and NodeManager, and Kyuubi servers that
 * run the jars built from this repository.
 *
 * Every container joins the network namespace of the KDC container, whose host name is
 * localhost, so that services, principals and certificates share one host name on any Docker
 * host, and the ports published by the KDC container reach the Kyuubi servers and ZooKeeper
 * from the test JVM. The file systems stay separate: a file copied into the NodeManager is not
 * visible to a Kyuubi server, as on a real cluster. Spark engines run as YARN containers of the
 * NodeManager, with the Java 17 runtime of the NodeManager image and the mounted Spark
 * distribution.
 *
 * The keytabs and the output of every container are kept under `directory`. When the cluster
 * stops, the YARN container logs and the engine launch logs of the Kyuubi servers are copied
 * there too.
 *
 * @param userGroups       groups of the session users and the Kyuubi user, mapped statically on
 *                         every node; the first group is the group of the user's home directory
 * @param nodeManagerFiles files copied into the NodeManager before it starts, by container path
 * @param kyuubiPorts      Thrift ports of the Kyuubi servers, published on the Docker host
 */
class KerberizedYarnCluster(
    directory: Path,
    build: KyuubiBuild,
    userGroups: Map[String, Seq[String]],
    nodeManagerFiles: Map[String, Path],
    kyuubiPorts: Seq[Int]) extends Logging {

  private val keytabDir = createDirectory(directory.resolve("keytabs"))
  private val logDir = createDirectory(directory.resolve("logs"))

  private lazy val kdcImage = new ImageFromDockerfile("kyuubi-it-kdc:1", false)
    .withFileFromClasspath("Dockerfile", s"$RESOURCES/kdc/Dockerfile")
    .withFileFromClasspath("entrypoint.sh", s"$RESOURCES/kdc/entrypoint.sh")
    .withFileFromClasspath("create-keytab.sh", s"$RESOURCES/kdc/create-keytab.sh")

  private lazy val hadoopImage = new ImageFromDockerfile("kyuubi-it-hadoop:1", false)
    .withFileFromClasspath("Dockerfile", s"$RESOURCES/hadoop/Dockerfile")
    .withBuildArg("HADOOP_IMAGE", image("KYUUBI_IT_HADOOP_IMAGE", "apache/hadoop:3.4.1"))
    .withBuildArg("JRE_IMAGE", image("KYUUBI_IT_HADOOP_JRE_IMAGE", HADOOP_JRE_IMAGE))

  private lazy val kyuubiImage = new ImageFromDockerfile("kyuubi-it-kyuubi:1", false)
    .withFileFromClasspath("Dockerfile", s"$RESOURCES/kyuubi/Dockerfile")
    .withBuildArg("JRE_IMAGE", image("KYUUBI_IT_KYUUBI_JRE_IMAGE", "eclipse-temurin:17-jre-noble"))

  // in start order, stopped in reverse order
  private var containers = Seq.empty[(String, ClusterContainer)]
  private var kdc: ClusterContainer = _
  private var networkMode: String = _

  private var _nameNode: ClusterContainer = _
  private var _resourceManager: ClusterContainer = _
  private var _nodeManager: ClusterContainer = _

  def nameNode: ClusterContainer = _nameNode

  def resourceManager: ClusterContainer = _resourceManager

  def nodeManager: ClusterContainer = _nodeManager

  /** Start the KDC, ZooKeeper, HDFS and YARN, and create the home directories of the users. */
  def start(): Unit = {
    try {
      startKdc()
      createKeytab(
        HADOOP_KEYTAB,
        NAME_NODE_PRINCIPAL,
        DATA_NODE_PRINCIPAL,
        RESOURCE_MANAGER_PRINCIPAL,
        NODE_MANAGER_PRINCIPAL,
        HTTP_PRINCIPAL)
      createKeytab(KYUUBI_KEYTAB, KYUUBI_PRINCIPAL)
      createKeytab(ADMIN_KEYTAB, ADMIN_PRINCIPAL)

      startContainer("zookeeper", zookeeper())

      _nameNode = startContainer(
        "namenode",
        hadoopDaemon("512m", "hdfs", "namenode")
          .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-hadoop/dfs/name")
          .waitingFor(logMessage(".*NameNode RPC up at.*")))
      startContainer(
        "datanode",
        hadoopDaemon("384m", "hdfs", "datanode")
          .waitingFor(logMessage(".*successfully registered with NN.*")))
      waitUntil("HDFS with one live DataNode out of safe mode") {
        val report = asAdmin(nameNode, "hdfs dfsadmin -report -live && hdfs dfsadmin -safemode get")
        report.contains("Live datanodes (1)") && report.contains("Safe mode is OFF")
      }
      createHomeDirectories()

      _resourceManager = startContainer(
        "resourcemanager",
        hadoopDaemon("512m", "yarn", "resourcemanager")
          .waitingFor(logMessage(".*Transitioned to active state.*")))
      val nodeManagerContainer = hadoopDaemon("384m", "yarn", "nodemanager")
        .withFileSystemBind(build.sparkHome.toString, SPARK_HOME, BindMode.READ_ONLY)
        .waitingFor(logMessage(".*Registered with ResourceManager as.*"))
      nodeManagerFiles.foreach { case (target, source) =>
        nodeManagerContainer.withCopyFileToContainer(
          MountableFile.forHostPath(source, READABLE_FILE_MODE),
          target)
      }
      _nodeManager = startContainer("nodemanager", nodeManagerContainer)
      waitUntil("YARN with one running NodeManager") {
        asAdmin(resourceManager, "yarn node -list -states RUNNING").contains("Total Nodes:1")
      }
    } catch {
      case NonFatal(e) =>
        stop()
        throw e
    }
  }

  /**
   * Start a Kyuubi server from the jars of this repository, with a kyuubi-defaults.conf made of
   * the cluster settings and `conf`, and the given files copied in by container path.
   */
  def startKyuubiServer(
      name: String,
      port: Int,
      conf: Map[String, String],
      files: Map[String, Path]): ClusterContainer = {
    require(kyuubiPorts.contains(port), s"Port $port is not published, use one of $kyuubiPorts")
    val container = joinNetwork(new ClusterContainer(kyuubiImage))
      .withEnv("KYUUBI_HOME", KYUUBI_HOME)
      .withEnv("KYUUBI_CONF_DIR", s"$KYUUBI_HOME/conf")
      .withEnv("KYUUBI_LOG_DIR", s"$KYUUBI_TMP/logs")
      .withEnv("KYUUBI_PID_DIR", s"$KYUUBI_TMP/pid")
      .withEnv("KYUUBI_WORK_DIR_ROOT", s"$KYUUBI_TMP/work")
      .withEnv("KYUUBI_JAVA_OPTS", "-Xmx512m")
      .withEnv("SPARK_HOME", SPARK_HOME)
      .withEnv("SPARK_CONF_DIR", s"$KYUUBI_HOME/spark-conf")
      .withEnv("HADOOP_CONF_DIR", s"$KYUUBI_HOME/hadoop-conf")
      .withWorkingDirectory("/tmp")
      .withCopyToContainer(Transferable.of(krb5Conf), KRB5_CONF)
      .withCopyToContainer(
        Transferable.of(kyuubiDefaults(port, conf)),
        s"$KYUUBI_HOME/conf/kyuubi-defaults.conf")
      .withCopyFileToContainer(
        MountableFile.forClasspathResource(s"$RESOURCES/kyuubi/log4j2.xml", READABLE_FILE_MODE),
        s"$KYUUBI_HOME/conf/log4j2.xml")
      .withCopyToContainer(
        Transferable.of("# the Spark settings of the engines are in kyuubi-defaults.conf\n"),
        s"$KYUUBI_HOME/spark-conf/spark-defaults.conf")
      .withFileSystemBind(keytabDir.toString, KEYTAB_DIR, BindMode.READ_ONLY)
      .withFileSystemBind(build.binDir.toString, s"$KYUUBI_HOME/bin", BindMode.READ_ONLY)
      .withFileSystemBind(build.jarsDir.toString, s"$KYUUBI_HOME/jars", BindMode.READ_ONLY)
      .withFileSystemBind(
        build.sparkEngineJar.toString,
        s"$KYUUBI_HOME/externals/engines/spark/${build.sparkEngineJar.getFileName}",
        BindMode.READ_ONLY)
      .withFileSystemBind(build.sparkHome.toString, SPARK_HOME, BindMode.READ_ONLY)
      .withCommand(s"$KYUUBI_HOME/bin/kyuubi", "run")
      .waitingFor(logMessage(".*Service\\[KyuubiServer\\] is started.*"))
    hadoopConfFiles.foreach { case (fileName, content) =>
      container.withCopyToContainer(Transferable.of(content), s"$KYUUBI_HOME/hadoop-conf/$fileName")
    }
    files.foreach { case (target, source) =>
      container.withCopyFileToContainer(
        MountableFile.forHostPath(source, READABLE_FILE_MODE),
        target)
    }
    startContainer(name, container)
  }

  /** Stop all containers, after copying the YARN and engine launch logs to the log directory. */
  def stop(): Unit = {
    containers.foreach {
      case ("nodemanager", container) => archive(container, YARN_LOG_DIR, "yarn-container-logs")
      case (name, container) if name.startsWith("kyuubi") =>
        archive(container, s"$KYUUBI_TMP/work", s"$name-engine-launch-logs")
      case _ =>
    }
    containers.reverse.foreach { case (_, container) =>
      Utils.tryLogNonFatalError(container.stop())
    }
    containers = Nil
    info(s"The container logs of the kerberized YARN cluster are in $logDir")
  }

  /** Run a bash script in a container, failing unless it exits with 0. */
  def bash(container: ClusterContainer, script: String): String = {
    val result = container.execInContainer("bash", "-c", script)
    val output = result.getStdout + result.getStderr
    if (result.getExitCode != 0) {
      throw new IllegalStateException(
        s"`$script` failed with exit code ${result.getExitCode}:\n$output")
    }
    output
  }

  /** Run a bash script in a Hadoop container as the HDFS superuser and YARN administrator. */
  def asAdmin(container: ClusterContainer, script: String): String =
    bash(container, s"kinit -k -t $KEYTAB_DIR/$ADMIN_KEYTAB $ADMIN_PRINCIPAL && $script")

  /** Copy a host file into a running container, readable by every user. */
  def copyFile(container: ClusterContainer, source: Path, target: String): Unit =
    container.copyFileToContainer(MountableFile.forHostPath(source, READABLE_FILE_MODE), target)

  /** Paths of the files under the given container directories that contain `text`. */
  def filesContaining(container: ClusterContainer, text: String, dirs: String*): Seq[String] = {
    val result: ExecResult =
      container.execInContainer(Seq("grep", "-rlsF", "--", text) ++ dirs: _*)
    result.getStdout.split("\n").map(_.trim).filter(_.nonEmpty).toSeq
  }

  /** The `yarn application -status` report of an application. */
  def yarnApplication(appId: String): String =
    asAdmin(resourceManager, s"yarn application -status $appId")

  /** The `yarn application -list` table of the applications in the given states. */
  def yarnApplications(states: String*): String =
    asAdmin(resourceManager, s"yarn application -list -appStates ${states.mkString(",")}")

  private def startKdc(): Unit = {
    kdc = new ClusterContainer(kdcImage)
      .withEnv("KRB5_REALM", REALM)
      .withEnv("KDC_PORT", KDC_PORT.toString)
      .withCopyToContainer(Transferable.of(krb5Conf), KRB5_CONF)
      .withCreateContainerCmdModifier(new Consumer[CreateContainerCmd] {
        override def accept(cmd: CreateContainerCmd): Unit = cmd.withHostName(HOST)
      })
      .waitingFor(logMessage(".*Key Distribution Center ready.*"))
    kdc.setPortBindings((kyuubiPorts :+ ZOOKEEPER_PORT).map(p => s"$p:$p").asJava)
    startContainer("kdc", kdc)
    networkMode = s"container:${kdc.getContainerId}"
  }

  private def createKeytab(fileName: String, principals: String*): Unit = {
    val inContainer = s"/var/lib/krb5kdc/keytabs/$fileName"
    bash(kdc, (Seq("/usr/local/bin/create-keytab.sh", inContainer) ++ principals).mkString(" "))
    val keytab = keytabDir.resolve(fileName)
    kdc.copyFileFromContainer(inContainer, keytab.toString)
    setPermissions(keytab, "rw-r--r--")
  }

  private def zookeeper(): ClusterContainer = {
    val zooCfg = Seq(
      "tickTime=2000",
      "dataDir=/data",
      "dataLogDir=/datalog",
      s"clientPort=$ZOOKEEPER_PORT",
      "admin.enableServer=false").mkString("", "\n", "\n")
    joinNetwork(new ClusterContainer(
      new RemoteDockerImage(
        DockerImageName.parse(image("KYUUBI_IT_ZOOKEEPER_IMAGE", "zookeeper:3.9")))))
      .withCopyToContainer(Transferable.of(zooCfg), "/conf/zoo.cfg")
      .waitingFor(logMessage(s".*binding to port.*$ZOOKEEPER_PORT.*"))
  }

  private def hadoopDaemon(heap: String, command: String*): ClusterContainer = {
    val container = joinNetwork(new ClusterContainer(hadoopImage))
      .withEnv("HADOOP_HEAPSIZE_MAX", heap)
      .withCopyToContainer(Transferable.of(krb5Conf), KRB5_CONF)
      .withFileSystemBind(keytabDir.toString, KEYTAB_DIR, BindMode.READ_ONLY)
      .withCommand(command: _*)
    hadoopConfFiles.foreach { case (fileName, content) =>
      container.withCopyToContainer(Transferable.of(content), s"$HADOOP_IMAGE_CONF_DIR/$fileName")
    }
    container
  }

  private def createHomeDirectories(): Unit = {
    val users = userGroups.keys.toSeq.sorted
    val owners = users.map { user =>
      s"hdfs dfs -chown $user:${userGroups(user).headOption.getOrElse(user)} /user/$user"
    }
    val script = Seq(
      s"hdfs dfs -mkdir -p /tmp ${users.map(u => s"/user/$u").mkString(" ")}",
      "hdfs dfs -chmod 1777 /tmp") ++ owners :+
      s"hdfs dfs -chmod 700 ${users.map(u => s"/user/$u").mkString(" ")}"
    asAdmin(nameNode, script.mkString(" && "))
  }

  private def joinNetwork(container: ClusterContainer): ClusterContainer = {
    require(networkMode != null, "The KDC container owning the network is not started")
    val mode = networkMode
    container.withCreateContainerCmdModifier(new Consumer[CreateContainerCmd] {
      override def accept(cmd: CreateContainerCmd): Unit = cmd.getHostConfig.withNetworkMode(mode)
    })
  }

  private def startContainer(name: String, container: ClusterContainer): ClusterContainer = {
    container.withLogConsumer(new FileLogConsumer(logDir.resolve(s"$name.log")))
    containers :+= name -> container
    container.start()
    container
  }

  private def archive(container: ClusterContainer, dir: String, name: String): Unit = {
    Utils.tryLogNonFatalError {
      if (container.isRunning) {
        val path = Paths.get(dir)
        val tgz = s"/tmp/$name.tgz"
        val result = container.execInContainer(
          "tar",
          "-czf",
          tgz,
          "-C",
          path.getParent.toString,
          path.getFileName.toString)
        if (result.getExitCode == 0) {
          container.copyFileFromContainer(tgz, logDir.resolve(s"$name.tgz").toString)
        }
      }
    }
  }

  private def waitUntil(what: String)(ready: => Boolean): Unit = {
    val deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos
    var lastError: Throwable = null
    var done = false
    while (!done) {
      done =
        try ready
        catch {
          case NonFatal(e) =>
            lastError = e
            false
        }
      if (!done) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(s"$what not reached after $STARTUP_TIMEOUT", lastError)
        }
        Thread.sleep(POLL_INTERVAL.toMillis)
      }
    }
  }

  private lazy val hadoopConfFiles: Map[String, String] = Map(
    "core-site.xml" -> hadoopXml(coreSite),
    "hdfs-site.xml" -> hadoopXml(hdfsSite),
    "yarn-site.xml" -> hadoopXml(yarnSite),
    "capacity-scheduler.xml" -> hadoopXml(capacityScheduler))

  private def coreSite: Seq[(String, String)] = {
    val groups = Seq("dr.who=", "hadoop=supergroup") ++ userGroups.toSeq.sortBy(_._1).map {
      case (user, userGroupList) => s"$user=${userGroupList.mkString(",")}"
    }
    Seq(
      "fs.defaultFS" -> FS_URI,
      "hadoop.security.authentication" -> "kerberos",
      "hadoop.security.authorization" -> "false",
      "hadoop.rpc.protection" -> "authentication",
      "hadoop.security.auth_to_local" -> Seq(
        s"RULE:[2:$$1@$$0](nn@$REALM)s/.*/hadoop/",
        s"RULE:[2:$$1@$$0](dn@$REALM)s/.*/hadoop/",
        s"RULE:[1:$$1@$$0](admin@$REALM)s/.*/hadoop/",
        "DEFAULT").mkString("\n"),
      "hadoop.user.group.static.mapping.overrides" -> groups.mkString(";"),
      "hadoop.proxyuser.kyuubi.hosts" -> "*",
      "hadoop.proxyuser.kyuubi.groups" -> "*")
  }

  private def hdfsSite: Seq[(String, String)] = Seq(
    "dfs.replication" -> "1",
    "dfs.permissions.enabled" -> "true",
    "dfs.block.access.token.enable" -> "true",
    "ignore.secure.ports.for.testing" -> "true",
    "dfs.data.transfer.protection" -> "authentication",
    "dfs.namenode.rpc-address" -> s"$HOST:$NAME_NODE_PORT",
    "dfs.namenode.rpc-bind-host" -> "0.0.0.0",
    "dfs.namenode.kerberos.principal" -> NAME_NODE_PRINCIPAL,
    "dfs.namenode.kerberos.principal.pattern" -> "*",
    "dfs.namenode.keytab.file" -> s"$KEYTAB_DIR/$HADOOP_KEYTAB",
    "dfs.namenode.kerberos.internal.spnego.principal" -> HTTP_PRINCIPAL,
    "dfs.datanode.kerberos.principal" -> DATA_NODE_PRINCIPAL,
    "dfs.datanode.keytab.file" -> s"$KEYTAB_DIR/$HADOOP_KEYTAB",
    "dfs.datanode.hostname" -> HOST,
    "dfs.client.use.datanode.hostname" -> "true",
    "dfs.web.authentication.kerberos.principal" -> HTTP_PRINCIPAL,
    "dfs.web.authentication.kerberos.keytab" -> s"$KEYTAB_DIR/$HADOOP_KEYTAB")

  private def yarnSite: Seq[(String, String)] = Seq(
    "yarn.resourcemanager.hostname" -> HOST,
    "yarn.resourcemanager.principal" -> RESOURCE_MANAGER_PRINCIPAL,
    "yarn.resourcemanager.keytab" -> s"$KEYTAB_DIR/$HADOOP_KEYTAB",
    "yarn.nodemanager.principal" -> NODE_MANAGER_PRINCIPAL,
    "yarn.nodemanager.keytab" -> s"$KEYTAB_DIR/$HADOOP_KEYTAB",
    "yarn.nodemanager.container-executor.class" ->
      "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor",
    "yarn.nodemanager.local-dirs" -> YARN_LOCAL_DIR,
    "yarn.nodemanager.log-dirs" -> YARN_LOG_DIR,
    "yarn.nodemanager.delete.debug-delay-sec" -> "3600",
    "yarn.log-aggregation-enable" -> "false",
    "yarn.nodemanager.resource.memory-mb" -> "8192",
    "yarn.nodemanager.resource.cpu-vcores" -> "8",
    "yarn.scheduler.minimum-allocation-mb" -> "128",
    "yarn.scheduler.maximum-allocation-mb" -> "4096",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false",
    // the disk of a Docker VM is often fuller than the default 90%
    "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage" -> "100.0",
    "yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb" -> "0",
    "yarn.acl.enable" -> "false",
    "yarn.timeline-service.enabled" -> "false")

  // the default AM share of 10% does not fit one Spark driver
  private def capacityScheduler: Seq[(String, String)] = Seq(
    "yarn.scheduler.capacity.root.queues" -> "default",
    "yarn.scheduler.capacity.root.default.capacity" -> "100",
    "yarn.scheduler.capacity.root.default.maximum-capacity" -> "100",
    "yarn.scheduler.capacity.root.default.state" -> "RUNNING",
    "yarn.scheduler.capacity.maximum-am-resource-percent" -> "1.0")

  private def kyuubiDefaults(port: Int, conf: Map[String, String]): String = {
    val clusterConf = Map(
      "kyuubi.frontend.protocols" -> "THRIFT_BINARY",
      "kyuubi.frontend.bind.host" -> "0.0.0.0",
      "kyuubi.frontend.advertised.host" -> HOST,
      "kyuubi.frontend.thrift.binary.bind.port" -> port.toString,
      "kyuubi.authentication" -> "NONE",
      "kyuubi.metrics.enabled" -> "false",
      "kyuubi.operation.log.dir.root" -> s"$KYUUBI_TMP/server_operation_logs",
      "kyuubi.kinit.principal" -> KYUUBI_PRINCIPAL,
      "kyuubi.kinit.keytab" -> s"$KEYTAB_DIR/$KYUUBI_KEYTAB",
      "kyuubi.ha.addresses" -> s"$HOST:$ZOOKEEPER_PORT",
      "kyuubi.engine.share.level" -> "USER",
      "kyuubi.session.engine.initialize.timeout" -> "PT8M",
      "spark.master" -> "yarn",
      "spark.submit.deployMode" -> "cluster",
      "spark.yarn.jars" -> s"local:$SPARK_HOME/jars/*",
      "spark.yarn.appMasterEnv.JAVA_HOME" -> JAVA17_HOME,
      "spark.executorEnv.JAVA_HOME" -> JAVA17_HOME,
      "spark.yarn.maxAppAttempts" -> "1",
      "spark.driver.memory" -> "768m",
      "spark.executor.memory" -> "512m",
      "spark.executor.instances" -> "1",
      "spark.executor.cores" -> "1",
      "spark.dynamicAllocation.enabled" -> "false",
      "spark.ui.enabled" -> "false",
      "spark.sql.catalogImplementation" -> "in-memory",
      "spark.sql.shuffle.partitions" -> "2",
      // All containers share one network namespace, where localhost resolves to the loopback
      // address: an engine has to listen on the address of the host name it registers.
      "spark.kyuubi.frontend.thrift.binary.bind.host" -> HOST)
    (clusterConf ++ conf).toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }
      .mkString("", "\n", "\n")
  }

  private def krb5Conf: String =
    s"""[libdefaults]
       |    default_realm = $REALM
       |    dns_lookup_realm = false
       |    dns_lookup_kdc = false
       |    dns_canonicalize_hostname = false
       |    rdns = false
       |    udp_preference_limit = 1
       |    ticket_lifetime = 24h
       |    renew_lifetime = 7d
       |    forwardable = true
       |[realms]
       |    $REALM = {
       |        kdc = $HOST:$KDC_PORT
       |        admin_server = $HOST
       |    }
       |[domain_realm]
       |    $HOST = $REALM
       |""".stripMargin
}

object KerberizedYarnCluster {
  val HOST = "localhost"
  val REALM = "KYUUBI.IT"
  val KDC_PORT = 10088
  val NAME_NODE_PORT = 8020
  val ZOOKEEPER_PORT = 12181
  val FS_URI = s"hdfs://$HOST:$NAME_NODE_PORT"

  val NAME_NODE_PRINCIPAL = s"nn/$HOST@$REALM"
  val DATA_NODE_PRINCIPAL = s"dn/$HOST@$REALM"
  val RESOURCE_MANAGER_PRINCIPAL = s"rm/$HOST@$REALM"
  val NODE_MANAGER_PRINCIPAL = s"nm/$HOST@$REALM"
  val HTTP_PRINCIPAL = s"HTTP/$HOST@$REALM"
  val KYUUBI_PRINCIPAL = s"kyuubi/$HOST@$REALM"
  // mapped to hadoop, the user of the NameNode and so the HDFS superuser
  val ADMIN_PRINCIPAL = s"admin@$REALM"

  val KEYTAB_DIR = "/etc/security/keytabs"
  val HADOOP_KEYTAB = "hadoop.keytab"
  val KYUUBI_KEYTAB = "kyuubi.keytab"
  val ADMIN_KEYTAB = "admin.keytab"
  val KRB5_CONF = "/etc/krb5.conf"

  val SPARK_HOME = "/opt/spark"
  val JAVA17_HOME = "/opt/java17"
  val KYUUBI_HOME = "/opt/kyuubi"
  // the working directory of the Kyuubi servers, with the engine launch logs under work/<user>
  val KYUUBI_TMP = "/tmp/kyuubi"
  val YARN_LOCAL_DIR = "/tmp/yarn-local"
  val YARN_LOG_DIR = "/tmp/yarn-logs"

  val STARTUP_TIMEOUT: Duration = Duration.ofMinutes(10)
  val POLL_INTERVAL: Duration = Duration.ofSeconds(2)

  private val RESOURCES = "kerberized-yarn"
  // the linux/amd64 manifest, matching the Hadoop image that is only published for linux/amd64
  private val HADOOP_JRE_IMAGE = "eclipse-temurin:17-jre-jammy@sha256:" +
    "6faecf4bbb90d538fd739b8dd7ba7c3fb69bc58905b654a3d112452b83e877cb"
  private val READABLE_FILE_MODE = Integer.valueOf(Integer.parseInt("644", 8))
  private val HADOOP_IMAGE_CONF_DIR = "/opt/hadoop/etc/hadoop"

  private def image(variable: String, default: String): String =
    sys.env.get(variable).filter(_.nonEmpty).getOrElse(default)

  private def logMessage(regex: String) =
    Wait.forLogMessage(regex, 1).withStartupTimeout(STARTUP_TIMEOUT)

  private def hadoopXml(properties: Seq[(String, String)]): String = {
    def escape(value: String): String =
      value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    properties.map { case (name, value) =>
      s"  <property><name>${escape(name)}</name><value>${escape(value)}</value></property>\n"
    }.mkString(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<configuration>\n",
      "",
      "</configuration>\n")
  }

  private def createDirectory(dir: Path): Path = {
    Files.createDirectories(dir)
    // the containers read the keytabs and write nothing here, but run as other users
    setPermissions(dir, "rwxrwxrwx")
    dir
  }

  private def setPermissions(path: Path, permissions: String): Unit = {
    if (path.getFileSystem.supportedFileAttributeViews().contains("posix")) {
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions))
    }
  }
}

/** A container of the cluster, with the self type Testcontainers needs. */
class ClusterContainer(image: Future[String]) extends GenericContainer[ClusterContainer](image)

/**
 * The outputs of this repository's build that the Kyuubi containers run: the Kyuubi scripts, the
 * jars of `kyuubi-assembly`, the Spark SQL engine jar, and the Spark distribution of the build.
 */
case class KyuubiBuild(binDir: Path, jarsDir: Path, sparkEngineJar: Path, sparkHome: Path)

object KyuubiBuild {
  private val SPARK_DIR = """spark-(\d+\.\d+)[^/]*""".r

  /**
   * Locate the build outputs under the repository root given by the system property
   * `kyuubi.it.root`. The Spark distribution is the one downloaded by the build, unless the
   * system property `kyuubi.it.spark.home` or the environment variable KYUUBI_IT_SPARK_HOME
   * names another one.
   */
  def locate(): KyuubiBuild = {
    val root = Paths.get(sys.props.getOrElse("kyuubi.it.root", "../..")).toAbsolutePath.normalize
    val scalaVersion = sys.props.getOrElse("kyuubi.it.scala.binary.version", "2.13")
    val hint = "Build them first: build/mvn package -DskipTests " +
      "-pl kyuubi-assembly,externals/kyuubi-spark-sql-engine -am with the profiles of this run"

    val binDir = root.resolve("bin")
    val jarsDir = root.resolve(s"kyuubi-assembly/target/scala-$scalaVersion/jars")
    require(
      Files.isRegularFile(binDir.resolve("kyuubi")) && Files.isDirectory(jarsDir) &&
        list(jarsDir).exists(_.getFileName.toString.startsWith("kyuubi-server_")),
      s"The Kyuubi server jars are not in $jarsDir. $hint")

    // the engine jar of each Spark minor version built, e.g. 3.5 -> ...-spark-3.5_2.13-<v>.jar
    val engineJarName = s"""kyuubi-spark-sql-engine-spark-(\\d+\\.\\d+)_$scalaVersion-.*\\.jar""".r
    val engineJars = list(root.resolve("externals/kyuubi-spark-sql-engine/target")).flatMap {
      jar =>
        jar.getFileName.toString match {
          case name @ engineJarName(version)
              if !name.endsWith("-tests.jar") && !name.endsWith("-sources.jar") =>
            Some(version -> jar)
          case _ => None
        }
    }.toMap
    require(
      engineJars.nonEmpty,
      s"No Spark SQL engine jar for Scala $scalaVersion in " +
        s"externals/kyuubi-spark-sql-engine/target. $hint")

    val sparkHomes = sys.props.get("kyuubi.it.spark.home")
      .orElse(sys.env.get("KYUUBI_IT_SPARK_HOME"))
      .filter(_.nonEmpty)
      .map(home => Seq(Paths.get(home)))
      .getOrElse {
        list(root.resolve("externals/kyuubi-download/target")).filter { dir =>
          Files.isDirectory(dir) && SPARK_DIR.pattern.matcher(dir.getFileName.toString).matches()
        }.sorted
      }
      .map(_.toAbsolutePath.normalize)
    val (sparkHome, engineJar) = sparkHomes.flatMap { home =>
      home.getFileName.toString match {
        case SPARK_DIR(version) => engineJars.get(version).map(home -> _)
        case _ if engineJars.size == 1 => Some(home -> engineJars.values.head)
        case _ => None
      }
    }.headOption.getOrElse(throw new IllegalArgumentException(
      s"No Spark distribution in ${sparkHomes.mkString("[", ", ", "]")} matches the engine " +
        s"jars for Spark ${engineJars.keys.toSeq.sorted.mkString(", ")}. Set " +
        "-Dkyuubi.it.spark.home or KYUUBI_IT_SPARK_HOME, or let the build download Spark."))
    require(
      Files.isRegularFile(sparkHome.resolve("bin/spark-submit")),
      s"$sparkHome is not a Spark distribution")
    KyuubiBuild(binDir, jarsDir, engineJar, sparkHome)
  }

  private def list(dir: Path): Seq[Path] = {
    if (Files.isDirectory(dir)) {
      val stream = Files.list(dir)
      try stream.iterator().asScala.toList
      finally stream.close()
    } else {
      Nil
    }
  }
}

/** Writes the output of a container to a file as it arrives. */
private class FileLogConsumer(file: Path) extends Consumer[OutputFrame] {
  private val writer: BufferedWriter = Files.newBufferedWriter(file, StandardCharsets.UTF_8)

  override def accept(frame: OutputFrame): Unit = synchronized {
    try {
      if (frame.getType == OutputFrame.OutputType.END) {
        writer.close()
      } else {
        writer.write(frame.getUtf8String)
        writer.flush()
      }
    } catch {
      // the container output stays available through the Docker API
      case _: IOException =>
    }
  }
}
