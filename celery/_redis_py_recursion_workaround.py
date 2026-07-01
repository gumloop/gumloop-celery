"""Backport of redis-py PR #3557 for redis-py < 6.0.0b2.

Faithful port of the upstream fix
(https://github.com/redis/redis-py/pull/3557). Introduces
``connect_check_health`` and ``on_connect_check_health`` on
``redis.connection.Connection`` and threads ``check_health`` through every
``send_command`` call inside ``on_connect_check_health``, so the inner
reconnect path triggered from ``check_health`` cannot re-trigger
``check_health`` recursively.

The bug
-------
On 2026-06-25, ~261 Celery workers crashed in production with::

    CRITICAL Unrecoverable error: RecursionError(
        'maximum recursion depth exceeded while calling a Python object')

during a Redis broker rolling replacement. Cause:

    connect() -> on_connect()
      -> send_command("CLIENT","SETINFO","LIB-NAME", lib_name)
      -> send_packed_command(check_health=True)
      -> check_health() -> _send_ping() -> send_command("PING", check_health=False)
      -> send_packed_command(check_health=False)
      -> if not self._sock: self.connect()           # <-- re-enters top
      ... (~1000 frames until Python stack limit kills the worker)

Why a monkeypatch (instead of a redis-py upgrade)
-------------------------------------------------
``kombu 5.5.x`` pins ``redis<=5.2.1``; ``celery 5.5.x`` pins
``kombu<5.6``. Adopting redis-py >= 6.0 (where the fix shipped) requires a
coordinated ``celery 5.6+ / kombu 5.6+ / redis-py 6.0+`` upgrade plus a
rebase of this fork onto upstream celery 5.6+. Until that lands, this
module is the tactical patch.

Differences from upstream
-------------------------
1. Applied as a monkeypatch via import side-effect from
   ``celery/__init__.py``; the upstream fix is a redis-py code change.
2. Logs a single WARNING per worker process the first time the
   suppressed-check_health reconnect actually fires. Upstream is silent.
   This restores a small observability signal: when a transient broker
   blip is silently recovered, the worker log line tells operators it
   happened. Subsequent reconnects log at DEBUG to avoid spam.

The patch is a no-op when:
  - ``redis-py`` is not installed;
  - ``redis-py >= 6.0.0b2`` (detected via ``Connection.connect_check_health``).

References
----------
- redis-py PR #3557: https://github.com/redis/redis-py/pull/3557
- redis-py issue #3745: https://github.com/redis/redis-py/issues/3745
- Linear GMLP-9206
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _apply_redis_py_recursion_workaround() -> None:
    """Patch ``redis.connection.Connection`` in place. Idempotent."""
    try:
        import redis.connection as _rc
    except ImportError:
        return

    # Upstream fix already present (redis-py >= 6.0.0b2). Nothing to do.
    if hasattr(_rc.Connection, "connect_check_health"):
        return

    # Don't double-patch on re-import.
    if getattr(_rc.Connection.send_packed_command, "_gumloop_patched", False):
        return

    import socket

    from redis._parsers import _RESP2Parser, _RESP3Parser
    from redis.credentials import UsernamePasswordCredentialProvider
    from redis.exceptions import (
        AuthenticationError,
        AuthenticationWrongNumberOfArgsError,
        ConnectionError,
        RedisError,
        ResponseError,
        TimeoutError,
    )
    from redis.utils import str_if_bytes

    # One-shot flag so we WARN once per worker process, then DEBUG.
    _warned_once = [False]

    def on_connect_check_health(self, check_health: bool = True) -> None:
        """Initialize the connection, authenticate and select a database.

        Verbatim copy of redis-py 5.2.1's ``AbstractConnection.on_connect``
        (https://github.com/redis/redis-py/blob/v5.2.1/redis/connection.py#L398-L487),
        modified to thread ``check_health`` through every ``send_command``
        call so the inner reconnect path can suppress ``check_health`` and
        break the recursion.
        """
        self._parser.on_connect(self)
        parser = self._parser

        auth_args = None
        if self.credential_provider or (self.username or self.password):
            cred_provider = (
                self.credential_provider
                or UsernamePasswordCredentialProvider(self.username, self.password)
            )
            auth_args = cred_provider.get_credentials()

        if auth_args and self.protocol not in [2, "2"]:
            if isinstance(self._parser, _RESP2Parser):
                self.set_parser(_RESP3Parser)
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            if len(auth_args) == 1:
                auth_args = ["default", auth_args[0]]
            self.send_command(
                "HELLO",
                self.protocol,
                "AUTH",
                *auth_args,
                check_health=check_health,
            )
            self.handshake_metadata = self.read_response()
        elif auth_args:
            # AUTH itself must not check health (the connection isn't
            # authenticated yet, so PING would fail). Preserved from upstream.
            self.send_command("AUTH", *auth_args, check_health=False)
            try:
                auth_response = self.read_response()
            except AuthenticationWrongNumberOfArgsError:
                self.send_command("AUTH", auth_args[-1], check_health=False)
                auth_response = self.read_response()
            if str_if_bytes(auth_response) != "OK":
                raise AuthenticationError("Invalid Username or Password")
        elif self.protocol not in [2, "2"]:
            if isinstance(self._parser, _RESP2Parser):
                self.set_parser(_RESP3Parser)
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            self.send_command("HELLO", self.protocol, check_health=check_health)
            self.handshake_metadata = self.read_response()
            if (
                self.handshake_metadata.get(b"proto") != self.protocol
                and self.handshake_metadata.get("proto") != self.protocol
            ):
                raise ConnectionError("Invalid RESP version")

        if self.client_name:
            self.send_command(
                "CLIENT", "SETNAME", self.client_name, check_health=check_health,
            )
            if str_if_bytes(self.read_response()) != "OK":
                raise ConnectionError("Error setting client name")

        try:
            if self.lib_name:
                self.send_command(
                    "CLIENT",
                    "SETINFO",
                    "LIB-NAME",
                    self.lib_name,
                    check_health=check_health,
                )
                self.read_response()
        except ResponseError:
            pass

        try:
            if self.lib_version:
                self.send_command(
                    "CLIENT",
                    "SETINFO",
                    "LIB-VER",
                    self.lib_version,
                    check_health=check_health,
                )
                self.read_response()
        except ResponseError:
            pass

        if self.db:
            self.send_command("SELECT", self.db, check_health=check_health)
            if str_if_bytes(self.read_response()) != "OK":
                raise ConnectionError("Invalid Database")

    def connect_check_health(self, check_health: bool = True) -> None:
        """Connect to the Redis server if not already connected.

        Verbatim copy of redis-py 5.2.1's ``AbstractConnection.connect``
        (https://github.com/redis/redis-py/blob/v5.2.1/redis/connection.py#L352-L387),
        modified to call ``on_connect_check_health(check_health=check_health)``
        instead of ``on_connect()``.
        """
        if self._sock:
            return
        try:
            sock = self.retry.call_with_retry(
                lambda: self._connect(),
                lambda error: self.disconnect(error),
            )
        except socket.timeout:
            raise TimeoutError("Timeout connecting to server")
        except OSError as e:
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        try:
            if self.redis_connect_func is None:
                self.on_connect_check_health(check_health=check_health)
            else:
                self.redis_connect_func(self)
        except RedisError:
            self.disconnect()
            raise

        # Run any user callbacks. The only internal callback today is
        # pubsub channel/pattern resubscription.
        self._connect_callbacks = [ref for ref in self._connect_callbacks if ref()]
        for ref in self._connect_callbacks:
            callback = ref()
            if callback:
                callback(self)

    def connect(self) -> None:
        """Backwards-compatible wrapper. Same shape as upstream PR #3557."""
        self.connect_check_health(check_health=True)

    def on_connect(self) -> None:
        """Backwards-compatible wrapper. Same shape as upstream PR #3557."""
        self.on_connect_check_health(check_health=True)

    _orig_send_packed_command = _rc.Connection.send_packed_command

    def send_packed_command(self, command, check_health: bool = True):
        """Same as the original ``send_packed_command``, but when reconnect
        is needed mid-command, suppress ``check_health`` during the inner
        reconnect to break the redis-py < 6.0.0b2 recursion.

        Also logs the reconnect for operator visibility (once per process
        at WARNING, then DEBUG).
        """
        if not self._sock:
            if not _warned_once[0]:
                _warned_once[0] = True
                logger.warning(
                    "[GMLP-9206] redis-py reconnect mid-command "
                    "(host=%s port=%s db=%s). Backport of redis-py PR #3557 "
                    "active; transient broker blip silently recovered. "
                    "This WARNING is emitted once per worker process; "
                    "subsequent reconnects log at DEBUG.",
                    getattr(self, "host", "?"),
                    getattr(self, "port", "?"),
                    getattr(self, "db", "?"),
                )
            else:
                logger.debug(
                    "[GMLP-9206] redis-py reconnect mid-command "
                    "(host=%s port=%s db=%s)",
                    getattr(self, "host", "?"),
                    getattr(self, "port", "?"),
                    getattr(self, "db", "?"),
                )
            self.connect_check_health(check_health=False)
        return _orig_send_packed_command(self, command, check_health=check_health)

    send_packed_command._gumloop_patched = True  # type: ignore[attr-defined]

    _rc.Connection.connect_check_health = connect_check_health
    _rc.Connection.on_connect_check_health = on_connect_check_health
    _rc.Connection.connect = connect
    _rc.Connection.on_connect = on_connect
    _rc.Connection.send_packed_command = send_packed_command


_apply_redis_py_recursion_workaround()
