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

    # HA failover is transparent - if the Kyuubi server crashes between queries,
    # FailoverChannel automatically switches to the next ZK server and refreshes
    # the token. The same spark variable keeps working:
    spark.sql("SELECT 1").show()  # server A
    # server A crashes
    spark.sql("SELECT 2").show()  # FailoverChannel silently switches to server B
"""

import base64
import random
import grpc
# DefaultChannelBuilder exists since 3.4.0 and is the URL-parsing implementation on all
# supported versions. Plain ChannelBuilder is still importable in Spark 4.x but its __init__
# no longer takes a URL string there (that moved to DefaultChannelBuilder), so importing it
# directly would silently break Spark 4.x sessions.
from pyspark.sql.connect.client import DefaultChannelBuilder as ChannelBuilder
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
            if not username or not password:
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

    def retarget(self, host: str, port: int):
        """Point renew/revoke RPCs at a new server after failover.

        Only the RPC destination changes; the token stays valid cluster-wide via
        the shared JDBC token store.
        """
        self._host = host
        self._port = port

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


class FailoverChannel:
    """
    gRPC channel wrapper that automatically fails over to the next live Kyuubi server
    on UNAVAILABLE errors. Held by SparkConnectClient; when the server crashes, the
    next RPC through this channel transparently switches to the next ZK server -
    without the caller doing anything.

    The auth token is unchanged after failover because tokens are stored in the shared
    JDBC token store and validated by any Kyuubi server in the cluster.

    Transparent failover is only possible when no response data has been received yet
    (i.e. the new request hasn't started streaming). If UNAVAILABLE arrives mid-stream
    after data has already flowed, the error is re-raised so the caller can retry.
    """

    def __init__(self, builder: "KyuubiSessionBuilder"):
        self._builder = builder
        self._channel = builder._raw_channel()
        self._current_server = builder.endpoint

    def _do_failover(self, exclude: set):
        new_channel = self._builder._failover(self._current_server, exclude=exclude)
        self._channel.close()
        self._channel = new_channel
        self._current_server = self._builder.endpoint

    def unary_unary(self, method, request_serializer=None, response_deserializer=None,
                     **channel_kwargs):
        def callable_(*args, **kwargs):
            tried = {self._current_server}
            try:
                return self._channel.unary_unary(
                    method, request_serializer, response_deserializer,
                    **channel_kwargs)(*args, **kwargs)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    try:
                        self._do_failover(exclude=tried)
                    except RuntimeError:
                        raise e
                    return self._channel.unary_unary(
                        method, request_serializer, response_deserializer,
                        **channel_kwargs)(*args, **kwargs)
                raise
        return callable_

    def unary_stream(self, method, request_serializer=None, response_deserializer=None,
                      **channel_kwargs):
        def callable_(request, **kwargs):
            tried_servers = set()
            iterator = iter(self._channel.unary_stream(
                method, request_serializer, response_deserializer,
                **channel_kwargs)(request, **kwargs))

            def _gen():
                while True:
                    try:
                        yield next(iterator)
                    except StopIteration:
                        return
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            tried_servers.add(self._current_server)
                            try:
                                self._do_failover(exclude=tried_servers)
                            except RuntimeError:
                                pass
                        raise  # let ExecutePlanResponseReattachableIterator retry via ReattachExecute

            return _gen()
        return callable_

    def stream_unary(self, method, request_serializer=None, response_deserializer=None,
                      **channel_kwargs):
        return self._channel.stream_unary(
            method, request_serializer, response_deserializer, **channel_kwargs)

    def stream_stream(self, method, request_serializer=None, response_deserializer=None,
                       **channel_kwargs):
        return self._channel.stream_stream(
            method, request_serializer, response_deserializer, **channel_kwargs)

    def subscribe(self, callback, try_to_connect=False):
        return self._channel.subscribe(callback, try_to_connect)

    def unsubscribe(self, callback):
        return self._channel.unsubscribe(callback)

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class KyuubiSessionBuilder(ChannelBuilder):
    """Builder for a Kyuubi-authenticated Spark Connect session.

    Mirrors the JVM KyuubiSessionBuilder API: call getOrCreate() to get a SparkSession.

    Usage:
        spark = KyuubiSessionBuilder("sc://host:10199", auth="kerberos").getOrCreate()
        spark.stop() # sends ReleaseSession
    """

    @staticmethod
    def _parse_zk_url(url: str):
        """Parse a ZooKeeper discovery URL into its components.

        Returns (zk_addresses, zk_path, non_zk_params) if url uses
        serviceDiscoveryMode=zooKeeper, or None for plain sc://host:port URLs.
        """
        if not url.startswith("sc://"):
            return None
        rest = url[len("sc://"):]
        slash_idx = rest.find("/")
        if slash_idx == -1:
            return None
        zk_addresses = rest[:slash_idx]
        params_part = rest[slash_idx + 1:].lstrip(";")
        params = {}
        for part in params_part.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value
        if params.get("serviceDiscoveryMode") != "zooKeeper":
            return None
        namespace = params.get("zooKeeperNamespace", "kyuubi_sc")
        zk_path = "/" + namespace
        non_zk_params = {k: v for k, v in params.items()
                         if k not in ("serviceDiscoveryMode", "zooKeeperNamespace")}
        return zk_addresses, zk_path, non_zk_params

    @staticmethod
    def _resolve_url(url: str, exclude: set = None) -> str:
        """Resolve a ZooKeeper discovery URL to a direct sc://host:port URL.

        If URL contains serviceDiscoveryMode=zooKeeper, queries ZooKeeper for
        live Kyuubi Spark Connect servers under zooKeeperNamespace and picks one at random.
        Servers in `exclude` (format: "host:port") are skipped.
        Non-ZK parameters (e.g. use_ssl) are preserved in the resolved URL.
        Returns the URL unchanged if not in ZK discovery mode.
        """
        parsed = KyuubiSessionBuilder._parse_zk_url(url)
        if parsed is None:
            return url
        zk_addresses, zk_path, non_zk_params = parsed

        from kazoo.client import KazooClient
        from kazoo.exceptions import KazooException, NoNodeError
        zk = KazooClient(hosts=zk_addresses)
        zk.start()
        try:
            try:
                children = zk.get_children(zk_path)
                candidates = []
                for child in children:
                    try:
                        data, _ = zk.get(f"{zk_path}/{child}")
                    except NoNodeError:
                        continue  # node deleted between get_children and get
                    server = data.decode("utf-8")  # format: host:port
                    if exclude is None or server not in exclude:
                        candidates.append(server)
                if not candidates:
                    excluded_note = (f" (excluded: {', '.join(sorted(exclude))})"
                                     if exclude else "")
                    raise RuntimeError(
                        f"No Kyuubi Spark Connect servers found in ZooKeeper at {zk_path}"
                        + excluded_note)
                connect_url = random.choice(candidates)
            except KazooException as e:
                raise RuntimeError(f"ZooKeeper error during server resolution: {e}") from e
        finally:
            zk.stop()

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
        self._original_url = url
        self._auth = auth
        self._username = username
        self._password = password
        super().__init__(self._resolve_url(url))
        if auth == "none":
            self._kyuubi_client = None
        else:
            self._kyuubi_client = KyuubiTokenClient(self.host, self._grpc_port, self.secure)
            self._kyuubi_client.get_token(auth, username=username, password=password)

    @property
    def _grpc_port(self) -> int:
        """The connect port as an int.

        Spark 3.5's ChannelBuilder exposes `port` as a plain attribute; Spark 4.x's
        DefaultChannelBuilder made it private (`_port`) and only exposes the combined
        `endpoint` ("host:port") property. Fall back to parsing `endpoint` so this works
        on both.
        """
        port = getattr(self, "port", None)
        if port is not None:
            return port
        return int(self.endpoint.rsplit(":", 1)[1])

    def _raw_channel(self) -> grpc.Channel:
        """Create a plain gRPC channel to the current host:port (no failover wrapper)."""
        destination = self.endpoint
        if self.secure:
            return grpc.secure_channel(
                destination,
                grpc.ssl_channel_credentials(),
                options=self._CHANNEL_OPTIONS)
        return grpc.insecure_channel(destination, options=self._CHANNEL_OPTIONS)

    def _failover(self, failed_server: str, exclude: set = None) -> grpc.Channel:
        """Resolve the next live ZK server (excluding failed_server and any in exclude), return new channel.

        Token is NOT revoked or refreshed - the existing token remains valid on the new
        server because tokens are persisted in the shared JDBC token store.
        """
        effective_exclude = set(exclude) if exclude else set()
        effective_exclude.add(failed_server)
        new_url = self._resolve_url(self._original_url, exclude=effective_exclude)
        super(KyuubiSessionBuilder, self).__init__(new_url)
        if self._kyuubi_client is not None:
            # Renewal must follow the current live server, not the token's original issuer.
            self._kyuubi_client.retarget(self.host, self._grpc_port)
        return self._raw_channel()

    def toChannel(self) -> FailoverChannel:
        return FailoverChannel(self)

    def getOrCreate(self) -> SparkSession:
        session = SparkSession.builder.channelBuilder(self).getOrCreate()
        client = session.client
        if hasattr(client, "_retry_policy"):
            # Spark < 4.1: retry config is a plain dict.
            client._retry_policy.update({"max_retries": 3, "max_backoff": 5000})
        else:
            # Spark >= 4.1: retry config moved to a list of RetryPolicy objects.
            for policy in client._retry_policies:
                policy.max_retries = 3
                policy.max_backoff = 5000
        return session

    def metadata(self):
        base = list(super().metadata())
        if self._kyuubi_client is not None:
            base.append(("authorization", f"Bearer {self._kyuubi_client.token}"))
        return base

    def reconnect(self) -> SparkSession:
        """Explicit failover: mark current server failed, switch to next, return new SparkSession.

        Normally not needed - FailoverChannel handles failover transparently between queries.
        Use reconnect() to force a new SparkSession after a mid-stream failure.
        """
        self._failover(self.endpoint)
        return self.getOrCreate()

    def renew(self):
        if self._kyuubi_client is not None:
            self._kyuubi_client.renew()

    def revoke(self):
        if self._kyuubi_client is not None:
            self._kyuubi_client.revoke()
