#-*- coding=utf-8 -*-
"""
    Queue with key.
    Note that the key should not contain '/'(which will be used as seperator)
"""

# Note that the thread should be monkey patched
import threading
from time import time
from operator import attrgetter
from .buckets import TokenBucketQueue, FastQueue, RateLimitExceeded
from kikyo.utils.limits import TokenBucket
from kikyo.five import Empty, items, PriorityQueue, sleep
from kikyo.utils import timeutils
from kikyo.local import Proxy

class KQueueNotFound(Exception):
    pass

class KQueueMixin(object):
    __qkey__ = None
    def __init__(self, qkey=None, priority=0):
        self.__qkey__ = qkey
        self.priority = priority
    def find(self, qkey):
        if self.__qkey__ == qkey:
            return self
    def __repr__(self):
        return "KQueue:{0}".format(self.__qkey__)

class KFastQueue(KQueueMixin, FastQueue):
    def __init__(self, *args, **kw):
        FastQueue.__init__(self)
        KQueueMixin.__init__(self, *args, **kw)
    def __repr__(self):
        return "KFastQueue__{0}".format(self.__key__)

def isgroupqueue(kq):
    return isinstance(kq, KGroupQueueAbstract)
def isqueue(kq):
    return isinstance(kq, KQueueMixin)

class KTokenBucketQueue(KQueueMixin, TokenBucketQueue):
    def __init__(self, rate_limit, queue=None, capacity=1, **kw):
        TokenBucketQueue.__init__(self, rate_limit, queue, capacity)
        KQueueMixin.__init__(self, **kw)
    def __repr__(self):
        return "KTokenBucketQueue__{0}".format(self.__key__)

class KGroupQueueAbstract(object):
    def __init__(self, qkey=None):
        self.__lastput__ = -1
        self.__lastget__ = -1
        self.kqmap = {}
        self.__qkey__ = qkey or "anonymous"
        self.queues = Proxy(self._getqueues)
        self.immediate = PriorityQueue()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        
    def _nextput(self):
        queues = self._getqueues()
        if not queues:
            raise KQueueNotFound()
        cur_put  = (self.__lastput__ + 1) % len(queues)
        kq = queues[cur_put]
        self.__lastput__ = cur_put
        return kq

    def _nextget(self):
        queues = [q for q in self._getqueues() if not q.empty()]
        if not queues:
            raise Empty()
        cur_get = (self.__lastget__ + 1) % len(queues)
        kq = self.kqmap.values()[cur_get]
        self.__lastget__ = cur_get
        return kq

    def _next_kq(self, next, notfound_exc, key=None):
        if key is None:
            kq = next()
        else:
            kq = self.find(key)
            if not kq:
                raise notfound_exc()
        if isqueue(kq):
            return kq
        else:
            return kq._next_kq(next=next, notfound_exc=notfound_exc)

    def _kq_toget(self, key=None):
        """
        Find a kq to get
        """
        return self._next_kq(key=key, next=self._nextget, notfound_exc=KQueueNotFound)

    def _kq_toput(self, key=None):
        """
        Find a kq to put
        """
        return self._next_kq(key=key, next=self._nextput, notfound_exc=Empty)

    def put(self, item, key=None):
        kq = self._kq_toput(key)
        kq.put_nowait(item)
        # wake up a greenlet
        with self.mutex:
            self.not_empty.notify()

    put_nowait = put

    def _get_immediate(self):
        return self.immediate.get(block=False)

    def _get(self):
        try:
            return 0, self._get_immediate()
        except Empty:
            pass
        remaining_times = []
        queues = sorted(self._getqueues(), key=attrgetter("priority"))
        for q in queues:
            remaining = q.expected_time()
            if not remaining:
                try:
                    self.immediate.put((q.priority, q.get_nowait()))
                except Empty:
                    pass
                except RateLimitExceeded:
                    remaining_times.append(q.expected_time())
            else:
                remaining_times.append(remaining)

        try:
            return 0, self._get_immediate()
        except Empty:
            if not remaining_times:
                raise
            return min(remaining_times), None
        
    def get(self, block=True, timeout=None):
        tstart = time()
        get = self._get
        not_empty = self.not_empty
        with not_empty:
            while 1:
                try:
                    remaining_time, priority_item = get()
                except Empty:
                    if not block or (timeout and time() - tstart > timeout):
                        raise
                    not_empty.wait(timeout)
                    continue
                if remaining_time:
                    if not block or (timeout and time() - tstart > timeout):
                        raise Empty()
                    sleep(min(remaining_time, timeout or 1))
                else:
                    return priority_item[1]

    def get_nowait(self):
        return self.get(block=False)

    def touch(self, key, rate_limits=None, capacities=None, priority=0):
        """
        Make sub groups and queue, the deepest queue will be returned
        """
        if key:
            keys = key.split('/')
            qkey = keys[-1]
            groupkeys = keys[0:-1]
            que = self.find(qkey) or KQueue.make(
                qkey=qkey,
                rate_limit=rate_limits[-1] if rate_limits else None,
                capacity=capacities[-1] if capacities else None,
                priority=priority)
            if groupkeys:
                groupqueue = self.makegroup(
                    '/'.join(groupkeys),
                    rate_limits[0:-1] if rate_limits else None,
                    capacities[0:-1] if capacities else None)
            else:
                groupqueue = self
            groupqueue.addque(qkey, que)
            return que
    def makegroup(self, key, rate_limits=None, capacities=None):
        """
        Make sub groups, the deepest group will be returned
        """
        if key:
            keys = key.split('/')
            qkey = keys[0]
            que = self.find(qkey) or KGroupQueue.make(
                qkey=qkey,
                rate_limit=rate_limits[0] if rate_limits else None,
                capacity=capacities[0] if capacities else None)
            if len(keys) > 1:
                subgroup = que.makegroup(
                    '/'.join(keys[1:]),
                    rate_limits[1:] if rate_limits else None,
                    capacities[-1] if capacities else None)
                self.addque(qkey, que)
                return subgroup
            else:
                self.addque(qkey, que)
                return que
    def addque(self, qkey, queue, update=False):
        """
        Directly add queue to the current queue group.
        If update is False, the queue will not be updated if it already
        exists in the kqmap.
        This is a low level interface, use it cautiously
        """
        if update or (qkey not in self.kqmap):
            self.kqmap[qkey] = queue

    def find(self, key):
        """
        find the group that matches the key
        """
        if key:
            keys = key.split('/')
            cur_key = keys[0]
            if len(keys) == 1:
                return self.kqmap.get(cur_key)
            elif cur_key in self.kqmap:
                return self.kqmap[cur_key].find('/'.join(keys[1:]))
    def __repr__(self):
        output = "%s{" % self.__qkey__
        for key, que in items(self.kqmap):
            output += "{0}".format(repr(que))
        output += "}"
        return output
    def get_priority(self):
        try:
            return min(q.priority for q in self.queues)
        except ValueError:
            # No queues in the group
            return -1
    def set_priority(self, priority):
        for q in self.queues:
            q.priority = priority
    priority = property(get_priority, set_priority)
    def _getqueues(self):
        """
        Return all the queues of the group
        Don't call self.queues in the func
        """
        queues = []
        for key, que in items(self.kqmap):
            if isqueue(que):
                queues.append(que)
            else:
                queues.extend(que._getqueues())
        return queues
    def qsize(self):
        return sum(q.qsize() for q in self.queues)
    def empty(self):
        return all(q.empty() for q in self.queues)
    def clear(self):
        for q in self.queues:
            q.clear()
class KFastGroupQueue(KGroupQueueAbstract):
    KQueueCls = KFastQueue
    def __init__(self, *args, **kw):
        super(KFastGroupQueue, self).__init__(*args, **kw)

class KTokenBucketGroupQueue(KGroupQueueAbstract):
    KQueueCls = KTokenBucketQueue
    def __init__(self, rate_limit, capacity=1, *args, **kw):
        super(KTokenBucketGroupQueue, self).__init__(*args, **kw)
        self._bucket = TokenBucket(rate_limit, capacity)
    def get(self, block=True, timeout=None):
        if not self._bucket.can_consume(1):
            if block:
                sleep(min(timeout, self._bucket.expected_time()) if timeout else self._bucket.expected_time)
            else:
                raise RateLimitExceeded()
        return super(KTokenBucketGroupQueue, self).get(block=block, timeout=timeout)

class KQueue(object):
    @classmethod
    def make(cls, **kw):
        if 'rate_limit' in kw and kw['rate_limit']:
            kw['rate_limit'] = timeutils.rate(kw['rate_limit'])
            for key in ('capacity',):
                if key in kw and not kw[key]:
                    del kw[key]
            kq = KTokenBucketQueue(**kw)
        else:
            for key in ('rate_limit', 'capacity'):
                if key in kw:
                    del kw[key]
            kq = KFastQueue(**kw)
        return kq

class KGroupQueue(object):
    @classmethod
    def make(cls, **kw):
        if 'rate_limit' in kw and kw['rate_limit']:
            kw['rate_limit'] = timeutils.rate(kw['rate_limit'])
            for key in ('capacity',):
                if key in kw and not kw[key]:
                    del kw[key]
            groupqueue = KTokenBucketGroupQueue(**kw)
        else:
            for key in ('rate_limit', 'capacity'):
                if key in kw:
                    del kw[key]
            groupqueue = KFastGroupQueue(**kw)
        return groupqueue
