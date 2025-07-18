import os
from collections.abc import Iterable
from time import sleep

from pydantic import BaseModel

from celery import Signature, Task, chain, chord, group, shared_task
from celery.canvas import signature
from celery.exceptions import SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

LEGACY_TASKS_DISABLED = True
try:
    # Imports that are not available in Celery 4
    from celery.canvas import StampingVisitor
except ImportError:
    LEGACY_TASKS_DISABLED = False


def get_redis_connection():
    from redis import StrictRedis

    host = os.environ.get("REDIS_HOST", "localhost")
    port = os.environ.get("REDIS_PORT", 6379)
    return StrictRedis(host=host, port=port)


logger = get_task_logger(__name__)


@shared_task
def identity(x):
    """Return the argument."""
    return x


@shared_task
def add(x, y, z=None):
    """Add two or three numbers."""
    if z:
        return x + y + z
    else:
        return x + y


@shared_task
def mul(x: int, y: int) -> int:
    """Multiply two numbers"""
    return x * y


@shared_task
def write_to_file_and_return_int(file_name, i):
    with open(file_name, mode='a', buffering=1) as file_handle:
        file_handle.write(str(i)+'\n')

    return i


@shared_task(typing=False)
def add_not_typed(x, y):
    """Add two numbers, but don't check arguments"""
    return x + y


@shared_task(ignore_result=True)
def add_ignore_result(x, y):
    """Add two numbers."""
    return x + y


@shared_task
def raise_error(*args):
    """Deliberately raise an error."""
    raise ValueError("deliberate error")


@shared_task
def chain_add(x, y):
    (
        add.s(x, x) | add.s(y)
    ).apply_async()


@shared_task
def chord_add(x, y):
    chord(add.s(x, x), add.s(y)).apply_async()


@shared_task
def delayed_sum(numbers, pause_time=1):
    """Sum the iterable of numbers."""
    # Allow the task to be in STARTED state for
    # a limited period of time.
    sleep(pause_time)
    return sum(numbers)


@shared_task
def delayed_sum_with_soft_guard(numbers, pause_time=1):
    """Sum the iterable of numbers."""
    try:
        sleep(pause_time)
        return sum(numbers)
    except SoftTimeLimitExceeded:
        return 0


@shared_task
def tsum(nums):
    """Sum an iterable of numbers."""
    return sum(nums)


@shared_task
def xsum(nums):
    """Sum of ints and lists."""
    return sum(sum(num) if isinstance(num, Iterable) else num for num in nums)


@shared_task(bind=True)
def add_replaced(self, x, y):
    """Add two numbers (via the add task)."""
    raise self.replace(add.s(x, y))


@shared_task(bind=True)
def replace_with_chain(self, *args, link_msg=None):
    c = chain(identity.s(*args), identity.s())
    link_sig = redis_echo.s()
    if link_msg is not None:
        link_sig.args = (link_msg,)
        link_sig.set(immutable=True)
    c.link(link_sig)

    return self.replace(c)


@shared_task(bind=True)
def replace_with_chain_which_raises(self, *args, link_msg=None):
    c = chain(identity.s(*args), raise_error.s())
    link_sig = redis_echo.s()
    if link_msg is not None:
        link_sig.args = (link_msg,)
        link_sig.set(immutable=True)
    c.link_error(link_sig)

    return self.replace(c)


@shared_task(bind=True)
def replace_with_empty_chain(self, *_):
    return self.replace(chain())


@shared_task(bind=True)
def add_to_all(self, nums, val):
    """Add the given value to all supplied numbers."""
    subtasks = [add.s(num, val) for num in nums]
    raise self.replace(group(*subtasks))


@shared_task(bind=True)
def add_to_all_to_chord(self, nums, val):
    for num in nums:
        self.add_to_chord(add.s(num, val))
    return 0


@shared_task(bind=True)
def add_chord_to_chord(self, nums, val):
    subtasks = [add.s(num, val) for num in nums]
    self.add_to_chord(group(subtasks) | tsum.s())
    return 0


@shared_task
def print_unicode(log_message='håå®ƒ valmuefrø', print_message='hiöäüß'):
    """Task that both logs and print strings containing funny characters."""
    logger.warning(log_message)
    print(print_message)


@shared_task
def return_exception(e):
    """Return a tuple containing the exception message and sentinel value."""
    return e, True


@shared_task
def sleeping(i, **_):
    """Task sleeping for ``i`` seconds, and returning nothing."""
    sleep(i)


@shared_task(bind=True)
def ids(self, i):
    """Returns a tuple of ``root_id``, ``parent_id`` and
    the argument passed as ``i``."""
    return self.request.root_id, self.request.parent_id, i


@shared_task(bind=True)
def collect_ids(self, res, i):
    """Used as a callback in a chain or group where the previous tasks
    are :task:`ids`: returns a tuple of::

        (previous_result, (root_id, parent_id, i))
    """
    return res, (self.request.root_id, self.request.parent_id, i)


@shared_task(bind=True, default_retry_delay=1)
def retry(self, return_value=None):
    """Task simulating multiple retries.

    When return_value is provided, the task after retries returns
    the result. Otherwise it fails.
    """
    if return_value:
        attempt = getattr(self, 'attempt', 0)
        print('attempt', attempt)
        if attempt >= 3:
            delattr(self, 'attempt')
            return return_value
        self.attempt = attempt + 1

    raise self.retry(exc=ExpectedException(), countdown=5)


@shared_task(bind=True, default_retry_delay=1)
def retry_unpickleable(self, foo, bar, *, retry_kwargs):
    """Task that fails with an unpickleable exception and is retried."""
    raise self.retry(exc=UnpickleableException(foo, bar), **retry_kwargs)


@shared_task(bind=True, expires=120.0, max_retries=1)
def retry_once(self, *args, expires=None, max_retries=1, countdown=0.1):
    """Task that fails and is retried. Returns the number of retries."""
    if self.request.retries:
        return self.request.retries
    raise self.retry(countdown=countdown,
                     expires=expires,
                     max_retries=max_retries)


@shared_task(bind=True, max_retries=1)
def retry_once_priority(self, *args, expires=60.0, max_retries=1,
                        countdown=0.1):
    """Task that fails and is retried. Returns the priority."""
    if self.request.retries:
        return self.request.delivery_info['priority']
    raise self.retry(countdown=countdown,
                     max_retries=max_retries)


@shared_task(bind=True, max_retries=1)
def retry_once_headers(self, *args, max_retries=1,
                       countdown=0.1):
    """Task that fails and is retried. Returns headers."""
    if self.request.retries:
        return self.request.headers
    raise self.retry(countdown=countdown,
                     max_retries=max_retries)


@shared_task
def redis_echo(message, redis_key="redis-echo"):
    """Task that appends the message to a redis list."""
    redis_connection = get_redis_connection()
    redis_connection.rpush(redis_key, message)


@shared_task(bind=True)
def redis_echo_group_id(self, _, redis_key="redis-group-ids"):
    redis_connection = get_redis_connection()
    redis_connection.rpush(redis_key, self.request.group)


@shared_task
def redis_count(redis_key="redis-count"):
    """Task that increments a specified or well-known redis key."""
    redis_connection = get_redis_connection()
    redis_connection.incr(redis_key)


@shared_task(bind=True)
def second_order_replace1(self, state=False):
    redis_connection = get_redis_connection()
    if not state:
        redis_connection.rpush('redis-echo', 'In A')
        new_task = chain(second_order_replace2.s(),
                         second_order_replace1.si(state=True))
        raise self.replace(new_task)
    else:
        redis_connection.rpush('redis-echo', 'Out A')


@shared_task(bind=True)
def second_order_replace2(self, state=False):
    redis_connection = get_redis_connection()
    if not state:
        redis_connection.rpush('redis-echo', 'In B')
        new_task = chain(redis_echo.s("In/Out C"),
                         second_order_replace2.si(state=True))
        raise self.replace(new_task)
    else:
        redis_connection.rpush('redis-echo', 'Out B')


@shared_task(bind=True)
def build_chain_inside_task(self):
    """Task to build a chain.

    This task builds a chain and returns the chain's AsyncResult
    to verify that Asyncresults are correctly converted into
    serializable objects"""
    test_chain = (
        add.s(1, 1) |
        add.s(2) |
        group(
            add.s(3),
            add.s(4)
        ) |
        add.s(5)
    )
    result = test_chain()
    return result


class ExpectedException(Exception):
    """Sentinel exception for tests."""

    def __eq__(self, other):
        return (
            other is not None and
            isinstance(other, ExpectedException) and
            self.args == other.args
        )

    def __hash__(self):
        return hash(self.args)


class UnpickleableException(Exception):
    """Exception that doesn't survive a pickling roundtrip (dump + load)."""

    def __init__(self, foo, bar=None):
        if bar is None:
            # We define bar with a default value in the signature so that
            # it's easier to add a break point here to find out when the
            # exception is being unpickled.
            raise TypeError("bar must be provided")

        super().__init__(foo)
        self.bar = bar


@shared_task
def fail(*args):
    """Task that simply raises ExpectedException."""
    args = ("Task expected to fail",) + args
    raise ExpectedException(*args)


@shared_task()
def fail_unpickleable(foo, bar):
    """Task that raises an unpickleable exception."""
    raise UnpickleableException(foo, bar)


@shared_task(bind=True)
def fail_replaced(self, *args):
    """Replace this task with one which raises ExpectedException."""
    raise self.replace(fail.si(*args))


@shared_task(bind=True)
def return_priority(self, *_args):
    return "Priority: %s" % self.request.delivery_info['priority']


@shared_task(bind=True)
def return_properties(self):
    return self.request.properties


class ClassBasedAutoRetryTask(Task):
    name = 'auto_retry_class_task'
    autoretry_for = (ValueError,)
    retry_kwargs = {'max_retries': 1}
    retry_backoff = True

    def run(self):
        if self.request.retries:
            return self.request.retries
        raise ValueError()


# The signatures returned by these tasks wouldn't actually run because the
# arguments wouldn't be fulfilled - we never actually delay them so it's fine
@shared_task
def return_nested_signature_chain_chain():
    return chain(chain([add.s()]))


@shared_task
def return_nested_signature_chain_group():
    return chain(group([add.s()]))


@shared_task
def return_nested_signature_chain_chord():
    return chain(chord([add.s()], add.s()))


@shared_task
def return_nested_signature_group_chain():
    return group(chain([add.s()]))


@shared_task
def return_nested_signature_group_group():
    return group(group([add.s()]))


@shared_task
def return_nested_signature_group_chord():
    return group(chord([add.s()], add.s()))


@shared_task
def return_nested_signature_chord_chain():
    return chord(chain([add.s()]), add.s())


@shared_task
def return_nested_signature_chord_group():
    return chord(group([add.s()]), add.s())


@shared_task
def return_nested_signature_chord_chord():
    return chord(chord([add.s()], add.s()), add.s())


@shared_task
def rebuild_signature(sig_dict):
    sig_obj = Signature.from_dict(sig_dict)

    def _recurse(sig):
        if not isinstance(sig, Signature):
            raise TypeError(f"{sig!r} is not a signature object")
        # Most canvas types have a `tasks` attribute
        if isinstance(sig, (chain, group, chord)):
            for task in sig.tasks:
                _recurse(task)
        # `chord`s also have a `body` attribute
        if isinstance(sig, chord):
            _recurse(sig.body)
    _recurse(sig_obj)


@shared_task
def errback_old_style(request_id):
    redis_count(request_id)
    return request_id


@shared_task
def errback_new_style(request, exc, tb):
    redis_count(request.id)
    return request.id


@shared_task
def replaced_with_me():
    return True


class AddParameterModel(BaseModel):
    x: int
    y: int


class AddResultModel(BaseModel):
    result: int


@shared_task(pydantic=True)
def add_pydantic(data: AddParameterModel) -> AddResultModel:
    """Add two numbers, but with parameters and results using Pydantic model serialization."""
    value = data.x + data.y
    return AddResultModel(result=value)


@shared_task(pydantic=True)
def add_pydantic_string_annotations(data: "AddParameterModel") -> "AddResultModel":
    """Add two numbers, but with string-annotated Pydantic models (__future__.annotations bug)."""
    value = data.x + data.y
    return AddResultModel(result=value)


if LEGACY_TASKS_DISABLED:
    class StampOnReplace(StampingVisitor):
        stamp = {"StampOnReplace": "This is the replaced task"}

        def on_signature(self, sig, **headers) -> dict:
            return self.stamp

    class StampedTaskOnReplace(Task):
        """Custom task for stamping on replace"""

        def on_replace(self, sig):
            sig.stamp(StampOnReplace())
            return super().on_replace(sig)

    @shared_task(bind=True, base=StampedTaskOnReplace)
    def replace_with_stamped_task(self: StampedTaskOnReplace, replace_with=None):
        if replace_with is None:
            replace_with = replaced_with_me.s()
        self.replace(signature(replace_with))


@shared_task(soft_time_limit=2, time_limit=1)
def soft_time_limit_must_exceed_time_limit():
    pass
