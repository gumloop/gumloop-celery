"""Backport of redis-py PR #3557 for redis-py < 6.0.0b2.

When the broker connection drops mid-handshake (e.g. broker pod restart,
TCP RST), redis-py < 6.0.0b2 enters infinite recursion through:

    on_connect() -> send_command("CLIENT","SETINFO",...)
      -> send_packed_command(check_health=True)
        -> check_health() -> _send_ping()
          -> send_command("PING", check_health=False)
            -> send_packed_command(check_health=False)
              -> if not self._sock: self.connect()
                -> on_connect() ...  (RecursionError at ~1000 frames)

The fix in redis-py PR #3557 (released in v6.0.0b2, April 2025) splits
``connect`` and ``on_connect`` into ``connect_check_health`` /
``on_connect_check_health`` and threads ``check_health=False`` through
the inner reconnect path so the cycle cannot form.

Because kombu < 5.6.0 pins ``redis<=5.2.1``, gumloop-celery users cannot
adopt the fix without a coordinated celery + kombu + redis-py upgrade.
Until that upgrade lands, this module applies the minimal localized
patch needed to break the recursion at the call site.

The patch:

- replaces ``Connection.send_packed_command``'s "no socket -> connect()"
  branch with a clean ``ConnectionError``. Higher-level callers
  (kombu's Channel, redis.client.Redis, application pools) already
  handle ``ConnectionError`` with their own retry/reconnect logic, so
  this surfaces the failure cleanly instead of recursing.

The patch is a no-op when redis-py is unavailable or when it already
contains the upstream fix (detected by ``connect_check_health`` method).

This module is imported from ``celery/__init__.py`` so the patch is
applied before any Connection is instantiated by kombu or application
code. Remove this module and its import after celery is upgraded to a
version that allows ``redis-py >= 6.0.0`` transitively (celery >= 5.6,
kombu >= 5.6).

References:

- redis-py PR #3557: https://github.com/redis/redis-py/pull/3557
- redis-py issue #3745: https://github.com/redis/redis-py/issues/3745
- Linear GMLP-9206 (Redis-broker RecursionError crash, 261 restarts).
"""

from __future__ import annotations


def _apply_redis_py_recursion_workaround() -> None:
    """Patch ``redis.connection.Connection.send_packed_command`` in place.

    Idempotent and safe to call multiple times. No-op when redis-py is
    not importable or already contains the upstream fix.
    """
    try:
        import redis.connection as _rc
    except ImportError:
        return

    # Upstream PR #3557 introduces ``connect_check_health`` as a sibling
    # of ``connect``. Its presence is a reliable signal that the fix is
    # already in the running redis-py and our patch is unnecessary.
    if hasattr(_rc.Connection, 'connect_check_health'):
        return

    # Avoid double-patching if this module is re-imported.
    if getattr(_rc.Connection.send_packed_command, '_gumloop_patched', False):
        return

    _orig_send_packed_command = _rc.Connection.send_packed_command

    def send_packed_command(self, command, check_health=True):
        if not self._sock:
            raise _rc.ConnectionError(
                "Connection lost; send_packed_command will not auto-reconnect "
                "to avoid the redis-py < 6.0.0b2 on_connect recursion "
                "(see redis/redis-py#3557)."
            )
        return _orig_send_packed_command(
            self, command, check_health=check_health,
        )

    send_packed_command._gumloop_patched = True  # type: ignore[attr-defined]
    _rc.Connection.send_packed_command = send_packed_command


_apply_redis_py_recursion_workaround()
