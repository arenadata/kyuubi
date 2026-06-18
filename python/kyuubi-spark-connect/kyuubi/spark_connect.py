"""
Kyuubi Spark Connect client.

KyuubiTokenClient - obtains/renews/revokes session token via gRPC.
KyuubiSessionBuilder - builder for all Kyuubi auth types (none/kerberos/ldap).
Token is revoked on server-side when spark.stop() sends ReleaseSession.

Usage:
    from kyuubi.spark_connect import KyuubiSessionBuilder

    # no auth:
    spark = KyuubiSessionBuilder("sc://host:10199").getOrCreate()
    # Kerberos:
    spark = KyuubiSessionBuilder("sc://host:10199/;use_ssl=true", auth="kerberos").getOrCreate()
    # LDAP:
    spark = KyuubiSessionBuilder("sc://host:10199/;use_ssl=true",
                                  auth="ldap", username="john", password="secret").getOrCreate()

    # ZooKeeper HA (Kerberos):
    spark = KyuubiSessionBuilder(
        "sc://zk1:2181,zk2:2181,zk3:2181/;serviceDiscoveryMode=zooKeeper"
        ";zooKeeperNamespace=arenadata/cluster/4/kyuubi_sc;use_ssl=true",
        auth="kerberos").getOrCreate()

    spark.sql("SELECT current_user()").show()
    spark.stop()  # sends ReleaseSession, server revokes token automatically
"""

import base64
import random
import grpc
from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession

from kyuubi.spark_connect_auth_pb2 import GetTokenRequest, RenewTokenRequest, RevokeTokenRequest
from kyuubi.spark_connect_auth_pb2_grpc import SparkConnectAuthServiceStub


class KyuubiTokenClient:
    def __init__(self, host: str, port: int = 10199, ssl: bool = True):
        self._host = host
        self._port = port
        self._ssl = ssl
        self._token = None
        self._expires_at_ms = None

    @property
    def token(self) -> str:
        if self._token is None:
            raise RuntimeError("No token - call get_token() first")
        return self._token

    @property
    def expires_at_ms(self) -> int:
        return self._expires_at_ms

    def get_token(self, auth: str, username: str = None, password: str = None) -> str:
        if auth == "kerberos":
            metadata = [("authorization", f"Negotiate {self._spnego_token()}")]
        elif auth == "ldap":
            if not username or password is None:
                raise ValueError("username and password are required for LDAP auth")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            metadata = [("authorization", f"Basic {encoded}")]
        else:
            raise ValueError(f"Unknown auth mode: {auth!r}. Use 'kerberos' or 'ldap'.")

        channel = self._channel()
        try:
            resp = SparkConnectAuthServiceStub(channel).GetToken(
                GetTokenRequest(), metadata=metadata)
            self._token = resp.token
            self._expires_at_ms = resp.expires_at_ms
            return self._token
        finally:
            channel.close()

    def renew(self):
        channel = self._channel()
        try:
            resp = SparkConnectAuthServiceStub(channel).RenewToken(
                RenewTokenRequest(token=self.token))
            self._expires_at_ms = resp.expires_at_ms
        finally:
            channel.close()

    def revoke(self):
        channel = self._channel()
        try:
            SparkConnectAuthServiceStub(channel).RevokeToken(
                RevokeTokenRequest(token=self.token))
            self._token = None
            self._expires_at_ms = None
        finally:
            channel.close()

    def _channel(self):
        if self._ssl:
            return grpc.secure_channel(
                f"{self._host}:{self._port}", grpc.ssl_channel_credentials())
        return grpc.insecure_channel(f"{self._host}:{self._port}")

    def _spnego_token(self) -> str:
        import gssapi
        name = gssapi.Name(
            f"HTTP@{self._host}", name_type=gssapi.NameType.hostbased_service)
        return base64.b64encode(
            gssapi.SecurityContext(name=name, usage="initiate").step()).decode()


class KyuubiSessionBuilder(ChannelBuilder):
    """Builder for a Kyuubi-authenticated Spark Connect session.

    Mirrors the JVM KyuubiSessionBuilder API: call getOrCreate() to get a SparkSession.

    Usage:
        spark = KyuubiSessionBuilder("sc://host:10199", auth="kerberos").getOrCreate()
        spark.stop() # sends ReleaseSession
    """

    @staticmethod
    def _resolve_url(url: str) -> str:
        """Resolve a ZooKeeper discovery URL to a direct sc://host:port URL.

        If URL contains serviceDiscoveryMode=zooKeeper, queries ZooKeeper for
        live Kyuubi Spark Connect servers under zooKeeperNamespace and picks one at random.
        Non-ZK parameters (e.g. use_ssl) are preserved in the resolved URL.
        Returns the URL unchanged if not in ZK discovery mode.
        """
        if not url.startswith("sc://"):
            return url

        rest = url[len("sc://"):]
        slash_idx = rest.find("/")
        if slash_idx == -1:
            return url

        zk_addresses = rest[:slash_idx]
        params_part = rest[slash_idx + 1:].lstrip(";")

        params = {}
        for part in params_part.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value

        if params.get("serviceDiscoveryMode") != "zooKeeper":
            return url

        namespace = params.get("zooKeeperNamespace", "kyuubi_sc")
        zk_path = "/" + namespace

        from kazoo.client import KazooClient
        zk = KazooClient(hosts=zk_addresses)
        zk.start()
        try:
            children = zk.get_children(zk_path)
            if not children:
                raise RuntimeError(
                    f"No Kyuubi Spark Connect servers found in ZooKeeper at {zk_path}")
            node = random.choice(children)
            data, _ = zk.get(f"{zk_path}/{node}")
            connect_url = data.decode("utf-8")  # format: host:port
        finally:
            zk.stop()

        non_zk_params = {k: v for k, v in params.items()
                         if k not in ("serviceDiscoveryMode", "zooKeeperNamespace")}
        resolved = f"sc://{connect_url}"
        if non_zk_params:
            resolved += "/;" + ";".join(f"{k}={v}" for k, v in non_zk_params.items())
        return resolved

    _CHANNEL_OPTIONS = [
        ("grpc.max_reconnect_backoff_ms", 10000),
        ("grpc.initial_reconnect_backoff_ms", 1000),
        ("grpc.keepalive_time_ms", 15000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.max_inbound_message_length", 128 * 1024 * 1024),
    ]

    def __init__(self, url: str, auth: str = "none",
                 username: str = None, password: str = None):
        super().__init__(self._resolve_url(url))
        if auth == "none":
            self._kyuubi_client = None
        else:
            self._kyuubi_client = KyuubiTokenClient(self.host, self.port, self.secure)
            self._kyuubi_client.get_token(auth, username=username, password=password)

    def toChannel(self):
        destination = f"{self.host}:{self.port}"
        if self.secure:
            return grpc.secure_channel(
                destination,
                grpc.ssl_channel_credentials(),
                options=self._CHANNEL_OPTIONS)
        return grpc.insecure_channel(destination, options=self._CHANNEL_OPTIONS)

    def getOrCreate(self) -> SparkSession:
        session = SparkSession.builder.channelBuilder(self).getOrCreate()
        session.client._retry_policy.update({
            "max_retries": 3,
            "max_backoff": 5000,
        })
        return session

    def metadata(self):
        base = list(super().metadata())
        if self._kyuubi_client is not None:
            base.append(("authorization", f"Bearer {self._kyuubi_client.token}"))
        return base

    def renew(self):
        if self._kyuubi_client is not None:
            self._kyuubi_client.renew()

    def revoke(self):
        if self._kyuubi_client is not None:
            self._kyuubi_client.revoke()
