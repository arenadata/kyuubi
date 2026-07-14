"""
Unit tests for KyuubiSessionBuilder URL parsing/resolution and FailoverChannel logic.

Tests use mocks for ZooKeeper (kazoo) and gRPC channels - no real cluster needed.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

import grpc

# ---------------------------------------------------------------------------
# Mock pyspark before importing our module (pyspark not needed for unit tests)
# ---------------------------------------------------------------------------

class _ChannelBuilder:
    """Minimal ChannelBuilder stub that parses sc://host:port URLs."""
    def __init__(self, url):
        self._url = url
        rest = url.removeprefix("sc://")
        slash = rest.find("/")
        host_port = rest[:slash] if slash != -1 else rest
        colon = host_port.rfind(":")
        self.host = host_port[:colon] if colon != -1 else host_port
        self.port = int(host_port[colon + 1:]) if colon != -1 else 10199
        self.secure = "use_ssl=true" in url

    def metadata(self):
        return []


_mock_pyspark_client = MagicMock()
_mock_pyspark_client.ChannelBuilder = _ChannelBuilder

sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())
sys.modules.setdefault("pyspark.sql.connect", MagicMock())
sys.modules.setdefault("pyspark.sql.connect.client", _mock_pyspark_client)
sys.modules.setdefault("pyspark.sql.connect.session", MagicMock())

# Mock kazoo - real exception classes so except-clauses work correctly
class _KazooException(Exception):
    pass


class _NoNodeError(_KazooException):
    pass


_mock_kazoo_exceptions = MagicMock()
_mock_kazoo_exceptions.KazooException = _KazooException
_mock_kazoo_exceptions.NoNodeError = _NoNodeError

sys.modules.setdefault("kazoo", MagicMock())
sys.modules.setdefault("kazoo.client", MagicMock())
sys.modules.setdefault("kazoo.exceptions", _mock_kazoo_exceptions)

from kyuubi.spark_connect import KyuubiSessionBuilder, FailoverChannel, KyuubiTokenClient  # noqa: E402


class _UnavailableError(grpc.RpcError):
    """Minimal grpc.RpcError with UNAVAILABLE status for tests."""
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "server unavailable"


# ---------------------------------------------------------------------------
# _parse_zk_url
# ---------------------------------------------------------------------------

class TestParseZkUrl(unittest.TestCase):

    def test_non_sc_url_returns_none(self):
        assert KyuubiSessionBuilder._parse_zk_url("jdbc:hive2://host:10009") is None

    def test_sc_url_without_discovery_mode_returns_none(self):
        assert KyuubiSessionBuilder._parse_zk_url("sc://host:10199/;use_ssl=true") is None

    def test_sc_url_with_other_discovery_mode_returns_none(self):
        assert KyuubiSessionBuilder._parse_zk_url(
            "sc://host:10199/;serviceDiscoveryMode=etcd") is None

    def test_sc_url_without_path_returns_none(self):
        assert KyuubiSessionBuilder._parse_zk_url("sc://host:10199") is None

    def test_zk_url_parsed_correctly(self):
        url = ("sc://zk1:2181,zk2:2181/;"
               "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi_sc")
        zk_addresses, zk_path, non_zk_params = KyuubiSessionBuilder._parse_zk_url(url)
        assert zk_addresses == "zk1:2181,zk2:2181"
        assert zk_path == "/kyuubi_sc"
        assert non_zk_params == {}

    def test_non_zk_params_preserved(self):
        url = ("sc://zk1:2181/;"
               "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns;use_ssl=true")
        _, _, non_zk_params = KyuubiSessionBuilder._parse_zk_url(url)
        assert non_zk_params == {"use_ssl": "true"}

    def test_default_namespace_is_kyuubi_sc(self):
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper"
        _, zk_path, _ = KyuubiSessionBuilder._parse_zk_url(url)
        assert zk_path == "/kyuubi_sc"


# ---------------------------------------------------------------------------
# _resolve_url (ZooKeeper interactions mocked via kazoo.client.KazooClient)
# ---------------------------------------------------------------------------

class TestResolveUrl(unittest.TestCase):

    def _patch_kazoo(self, zk_mock):
        """Patch KazooClient in the sys.modules entry so local imports inside _resolve_url
        pick it up. Returns a context manager."""
        return patch.object(sys.modules["kazoo.client"], "KazooClient",
                            return_value=zk_mock)

    def test_non_zk_url_returned_unchanged(self):
        url = "sc://host:10199/;use_ssl=true"
        assert KyuubiSessionBuilder._resolve_url(url) == url

    def test_resolves_to_registered_server(self):
        zk = _make_zk(children=["n1"], data={"n1": b"host1:10199"})
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns"
        with self._patch_kazoo(zk):
            result = KyuubiSessionBuilder._resolve_url(url)
        assert result == "sc://host1:10199"
        zk.stop.assert_called_once()

    def test_non_zk_params_preserved_in_resolved_url(self):
        zk = _make_zk(children=["n1"], data={"n1": b"host1:10199"})
        url = ("sc://zk1:2181/;"
               "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns;use_ssl=true")
        with self._patch_kazoo(zk):
            result = KyuubiSessionBuilder._resolve_url(url)
        assert result.startswith("sc://host1:10199")
        assert "use_ssl=true" in result
        assert "serviceDiscoveryMode" not in result
        assert "zooKeeperNamespace" not in result

    def test_excludes_specified_server(self):
        zk = _make_zk(children=["n1", "n2"],
                      data={"n1": b"hostA:10199", "n2": b"hostB:10199"})
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns"
        with self._patch_kazoo(zk):
            result = KyuubiSessionBuilder._resolve_url(url, exclude={"hostA:10199"})
        assert result == "sc://hostB:10199"

    def test_raises_when_all_candidates_excluded(self):
        zk = _make_zk(children=["n1"], data={"n1": b"host1:10199"})
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns"
        with self._patch_kazoo(zk):
            with self.assertRaises(RuntimeError):
                KyuubiSessionBuilder._resolve_url(url, exclude={"host1:10199"})

    def test_raises_when_no_servers_registered(self):
        zk = _make_zk(children=[], data={})
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns"
        with self._patch_kazoo(zk):
            with self.assertRaises(RuntimeError):
                KyuubiSessionBuilder._resolve_url(url)

    def test_kazoo_exception_wrapped_as_runtime_error(self):
        zk = MagicMock()
        zk.get_children.side_effect = _KazooException("connection lost")
        url = "sc://zk1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns"
        with self._patch_kazoo(zk):
            with self.assertRaises(RuntimeError):
                KyuubiSessionBuilder._resolve_url(url)


# ---------------------------------------------------------------------------
# FailoverChannel
# ---------------------------------------------------------------------------

class TestFailoverChannel(unittest.TestCase):

    def _make_channel(self, builder=None):
        """Create a FailoverChannel backed by a mock builder."""
        if builder is None:
            builder = _make_builder("host1", 10199)
        return FailoverChannel(builder)

    # unary_unary ---------------------------------------------------------

    def test_unary_unary_returns_response_on_success(self):
        channel = MagicMock()
        channel.unary_unary.return_value.return_value = "response"
        builder = _make_builder("host1", 10199, channel)
        fc = FailoverChannel(builder)
        result = fc.unary_unary("/method")("request")
        assert result == "response"

    def test_unary_unary_failover_on_unavailable_then_succeeds(self):
        channel1 = MagicMock()
        channel1.unary_unary.return_value.side_effect = _UnavailableError()

        channel2 = MagicMock()
        channel2.unary_unary.return_value.return_value = "response"

        builder = _make_builder("host1", 10199, channel1)
        builder._failover.return_value = channel2

        fc = FailoverChannel(builder)
        result = fc.unary_unary("/method")("request")
        assert result == "response"
        builder._failover.assert_called_once()

    def test_unary_unary_reraises_when_all_servers_exhausted(self):
        channel1 = MagicMock()
        channel1.unary_unary.return_value.side_effect = _UnavailableError()

        builder = _make_builder("host1", 10199, channel1)
        builder._failover.side_effect = RuntimeError("no servers left")

        fc = FailoverChannel(builder)
        with self.assertRaises(_UnavailableError):
            fc.unary_unary("/method")("request")

    def test_unary_unary_reraises_non_unavailable_error(self):
        class DeadlineError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED

        channel = MagicMock()
        channel.unary_unary.return_value.side_effect = DeadlineError()
        builder = _make_builder("host1", 10199, channel)
        fc = FailoverChannel(builder)

        with self.assertRaises(DeadlineError):
            fc.unary_unary("/method")("request")

    # unary_stream --------------------------------------------------------

    def test_unary_stream_yields_responses_on_success(self):
        channel = MagicMock()
        channel.unary_stream.return_value.return_value = iter(["r1", "r2", "r3"])
        builder = _make_builder("host1", 10199, channel)
        fc = FailoverChannel(builder)
        results = list(fc.unary_stream("/method")("request"))
        assert results == ["r1", "r2", "r3"]

    def test_unary_stream_switches_channel_and_reraises_on_unavailable(self):
        """UNAVAILABLE mid-stream: switches channel, then re-raises so
        ExecutePlanResponseReattachableIterator can retry via ReattachExecute."""
        error = _UnavailableError()

        def failing_stream(*args, **kwargs):
            yield "r1"
            raise error

        channel1 = MagicMock()
        channel1.unary_stream.return_value.return_value = failing_stream()

        channel2 = MagicMock()
        builder = _make_builder("host1", 10199, channel1)
        builder._failover.return_value = channel2

        fc = FailoverChannel(builder)
        gen = fc.unary_stream("/method")("request")

        assert next(gen) == "r1"
        with self.assertRaises(_UnavailableError):
            next(gen)

        builder._failover.assert_called_once()
        # channel switched - next RPC on fc will use channel2
        assert fc._channel is channel2

    def test_do_failover_closes_old_channel(self):
        old_channel = MagicMock()
        new_channel = MagicMock()
        builder = _make_builder("host1", 10199, old_channel)
        builder._failover.return_value = new_channel

        fc = FailoverChannel(builder)
        fc._do_failover(exclude=set())

        old_channel.close.assert_called_once()
        assert fc._channel is new_channel

    def test_do_failover_updates_current_server(self):
        channel1 = MagicMock()
        channel2 = MagicMock()
        builder = _make_builder("host1", 10199, channel1)

        def _side_effect(failed_server, exclude=None):
            builder.host = "host2"
            builder.port = 10200
            return channel2

        builder._failover.side_effect = _side_effect

        fc = FailoverChannel(builder)
        assert fc._current_server == "host1:10199"

        fc._do_failover(exclude=set())

        assert fc._current_server == "host2:10200"
        assert fc._channel is channel2

    def test_unary_stream_reraises_when_all_exhausted(self):
        """When _do_failover raises RuntimeError (no servers left), UNAVAILABLE is still re-raised."""
        error = _UnavailableError()

        def failing_stream(*args, **kwargs):
            raise error
            yield

        channel = MagicMock()
        channel.unary_stream.return_value.return_value = failing_stream()
        builder = _make_builder("host1", 10199, channel)
        builder._failover.side_effect = RuntimeError("no servers left")

        fc = FailoverChannel(builder)
        gen = fc.unary_stream("/method")("request")

        with self.assertRaises(_UnavailableError):
            next(gen)

        builder._failover.assert_called_once()

    def test_unary_stream_reraises_non_unavailable_error_immediately(self):
        """Non-UNAVAILABLE errors (e.g. INTERNAL for bad SQL / table not found) must
        propagate immediately. Before the fix, `raise` was inside the `if UNAVAILABLE`
        block so other errors fell through to `while True` and looped forever (freeze)."""
        class InternalError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.INTERNAL

            def details(self):
                return "PARSE_SYNTAX_ERROR: syntax error at or near 'sdfds'"

        error = InternalError()

        def failing_stream(*args, **kwargs):
            raise error
            yield

        channel = MagicMock()
        channel.unary_stream.return_value.return_value = failing_stream()
        builder = _make_builder("host1", 10199, channel)

        fc = FailoverChannel(builder)
        gen = fc.unary_stream("/method")("request")

        with self.assertRaises(InternalError):
            next(gen)

        builder._failover.assert_not_called()

    def test_unary_stream_accumulates_tried_servers(self):
        """tried_servers grows across multiple UNAVAILABLE errors in the same stream."""
        error = _UnavailableError()
        call_count = [0]

        def stream_that_fails_twice(*args, **kwargs):
            call_count[0] += 1
            raise error
            yield  # make it a generator

        channel1 = MagicMock()
        channel1.unary_stream.return_value.return_value = stream_that_fails_twice()

        builder = _make_builder("host1", 10199, channel1)
        builder._failover.return_value = channel1  # failover returns same channel (still failing)

        fc = FailoverChannel(builder)
        gen = fc.unary_stream("/method")("request")

        with self.assertRaises(_UnavailableError):
            next(gen)

        # _failover was called with the tried_servers set containing current server
        builder._failover.assert_called_once()
        _, kwargs = builder._failover.call_args
        assert "host1:10199" in kwargs.get("exclude", set())


# ---------------------------------------------------------------------------
# KyuubiSessionBuilder._failover
# ---------------------------------------------------------------------------

class TestKyuubiSessionBuilderFailover(unittest.TestCase):

    def test_failover_adds_failed_server_to_exclude(self):
        builder = KyuubiSessionBuilder("sc://host1:10199")
        captured = {}

        def fake_resolve(url, exclude=None):
            captured['exclude'] = set(exclude) if exclude else set()
            return "sc://host2:10199"

        with patch.object(KyuubiSessionBuilder, '_resolve_url', side_effect=fake_resolve):
            with patch.object(builder, '_raw_channel', return_value=MagicMock()):
                builder._failover("host1:10199", exclude={"prev:10199"})

        assert "host1:10199" in captured['exclude']
        assert "prev:10199" in captured['exclude']

    def test_failover_returns_new_channel(self):
        builder = KyuubiSessionBuilder("sc://host1:10199")
        new_channel = MagicMock()

        with patch.object(KyuubiSessionBuilder, '_resolve_url', return_value="sc://host2:10199"):
            with patch.object(builder, '_raw_channel', return_value=new_channel):
                result = builder._failover("host1:10199")

        assert result is new_channel

    def test_failover_updates_host_and_port(self):
        builder = KyuubiSessionBuilder("sc://host1:10199")

        with patch.object(KyuubiSessionBuilder, '_resolve_url', return_value="sc://host2:10200"):
            with patch.object(builder, '_raw_channel', return_value=MagicMock()):
                builder._failover("host1:10199")

        assert builder.host == "host2"
        assert builder.port == 10200

    def test_failover_retargets_token_client(self):
        """After failover, the token client must renew/revoke against the NEW live
        server, not the token's original issuer (dead server)."""
        with patch.object(KyuubiTokenClient, 'get_token', return_value="tok"):
            builder = KyuubiSessionBuilder("sc://host1:10199", auth="kerberos")

        assert builder._kyuubi_client._host == "host1"
        assert builder._kyuubi_client._port == 10199

        with patch.object(KyuubiSessionBuilder, '_resolve_url', return_value="sc://host2:10200"):
            with patch.object(builder, '_raw_channel', return_value=MagicMock()):
                builder._failover("host1:10199")

        assert builder._kyuubi_client._host == "host2"
        assert builder._kyuubi_client._port == 10200


# ---------------------------------------------------------------------------
# KyuubiSessionBuilder
# ---------------------------------------------------------------------------

class TestKyuubiSessionBuilder(unittest.TestCase):

    def test_metadata_includes_bearer_token(self):
        builder = KyuubiSessionBuilder("sc://host:10199")  # auth="none"
        mock_client = MagicMock()
        mock_client.token = "test-token-abc"
        builder._kyuubi_client = mock_client

        meta = dict(builder.metadata())
        assert meta.get("authorization") == "Bearer test-token-abc"

    def test_metadata_without_auth_has_no_bearer(self):
        builder = KyuubiSessionBuilder("sc://host:10199")  # auth="none"
        assert not any(k == "authorization" for k, _ in builder.metadata())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_builder(host: str, port: int, channel: MagicMock = None) -> MagicMock:
    builder = MagicMock(spec=KyuubiSessionBuilder)
    builder.host = host
    builder.port = port
    builder._raw_channel.return_value = channel or MagicMock()
    return builder


def _make_zk(children: list, data: dict) -> MagicMock:
    """Create a mock KazooClient instance returning given children and node data."""
    zk = MagicMock()
    zk.get_children.return_value = children

    def _get(path):
        node = path.rsplit("/", 1)[-1]
        return data[node], None

    zk.get.side_effect = _get
    return zk


if __name__ == "__main__":
    unittest.main()
