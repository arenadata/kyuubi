"""
Kyuubi Spark Connect client.

KyuubiTokenClient - obtains/renews/revokes session token via gRPC.
KyuubiChannelBuilder - PySpark ChannelBuilder for all auth types (none/kerberos/ldap).
    Token is revoked server-side when spark.stop() sends ReleaseSession.

Usage:
    from kyuubi.spark_connect import KyuubiChannelBuilder
    from pyspark.sql.connect.session import SparkSession

    # no auth:
    builder = KyuubiChannelBuilder("sc://host:10199")
    # Kerberos:
    builder = KyuubiChannelBuilder("sc://host:10199/;use_ssl=true", auth="kerberos")
    # LDAP:
    builder = KyuubiChannelBuilder("sc://host:10199/;use_ssl=true",
                                   auth="ldap", username="john", password="secret")

    spark = SparkSession(connection=builder)
    spark.sql("SELECT current_user()").show()
    spark.stop()  # sends ReleaseSession, server revokes token automatically
"""

import base64
import grpc
from pyspark.sql.connect.client import ChannelBuilder

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


class KyuubiChannelBuilder(ChannelBuilder):
    """PySpark ChannelBuilder for all Kyuubi auth types (none/kerberos/ldap).

    Revokes token on channel close for kerberos/ldap auth.

    Usage:
        builder = KyuubiChannelBuilder("sc://host:10199", auth="kerberos")
        spark = SparkSession(connection=builder)
        spark.stop()  # sends ReleaseSession + revokes token
    """

    def __init__(self, url: str, auth: str = "none",
                 username: str = None, password: str = None):
        super().__init__(url)
        if auth == "none":
            self._kyuubi_client = None
        else:
            self._kyuubi_client = KyuubiTokenClient(self.host, self.port, self.secure)
            self._kyuubi_client.get_token(auth, username=username, password=password)

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
