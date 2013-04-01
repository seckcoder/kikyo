#-*- coding=utf-8 -*-
"""
    Queue with key.
"""

from .buckets import TokenBucketQueue, FastQueue, RateLimitExceeded
from kikyo.utils.limits import TokenBucket
from kikyo.five import Queue, Empty
from kikyo.utils import timeutils

class KQueueNotFound(Exception):
    pass

class KQueueMixin(object):
    __qkey__ = None
    def __init__(self, qkey=None):
        self.__qkey__ = qkey
    def find(self, qkey):
        if self.__qkey__ == qkey:
            return self
    def __repr__(self):
        return "KQueue:{0}".format(self.__qkey__)
class KFastQueue(FastQueue, KQueueMixin):
    def __init__(self, qkey, *args, **kw):
        FastQueue.__init__(self, *args, **kw)
        KQueueMixin.__init__(self, qkey)

        
class KTokenBucketQueue(TokenBucketQueue, KQueueMixin):
    def __init__(self, qkey, **kw):
        self.formatkw(kw)
        TokenBucketQueue.__init__(self, **kw)
        KQueueMixin.__init__(self, qkey)
    def formatkw(self, kw):
        fill_rate = kw['rate_limit']
        kw['fill_rate'] = fill_rate
        del kw['rate_limit']
class KGroupQueueAbstract(object):
    def __init__(self, qkey=None):
        self.__lastput__ = -1
        self.__lastget__ = -1
        self.kqmap = {}
        self.__qkey__ = qkey or "anonymous"
    def nextput(self):
        if not self.kqmap:
            raise KQueueNotFound()
        cur_put  = (self.__lastput__ + 1) % len(self.kqmap)
        kq = self.kqmap.values()[cur_put]
        self.__lastput__ = cur_put
        return kq
    def nextget(self):
        if not self.kqmap:
            raise Empty()
        cur_get = (self.__lastget__ + 1) % len(self.kqmap)
        kq = self.kqmap.values()[cur_get]
        self.__lastget__ = cur_get
        return kq

    def put(self, item, block=True, key=None):
        if key is None:
            kq = self.nextput()
        else:
            kq = self.find(key)
            if not kq:
                raise KQueueNotFound("Queue {0} is not found".format(key))
        return kq.put(item, block=block)
    def put_nowait(self, item):
        return self.put(item, block=False)

    def _get(self, key=None, block=True):
        if key is None:
            kq = self.nextget()
        else:
            kq = self.find(key)
            if not kq:
                raise Empty('Key:{0} not found when get'.format(key))
        return kq.get(block=block)
    def get(self, key=None, block=True):
        return self._get(key, block=block)
    def get_nowait(self, key=None):
        return self.get(key=key, block=False)
    def touch(self, key, rate_limits=None):
        if key:
            keys = key.split('/')
            qkey = keys[-1]
            cur_rate = rate_limits[-1] if rate_limits else None
            groupkeys = keys[0:-1]
            groupqueue = self
            if groupkeys:
                groupqueue = self.makegroup('/'.join(groupkeys), rate_limits[0:-1] if rate_limits else None)
            groupqueue.addque(qkey, KQueue.make(qkey=qkey, rate_limit=cur_rate))
    def makegroup(self, key, rate_limits=None):
        if key:
            keys = key.split('/')
            qkey = keys[0]
            cur_rate = rate_limits[0] if rate_limits else None
            kq = KGroupQueue.make(rate_limit=cur_rate)
            kq.makegroup('/'.join(key[1:]), rate_limits[1:] if rate_limits else None)
            self.kqmap[qkey] = kq
    def addque(self, qkey, queue):
        """
        Directly add queue to the current queue group
        """
        self.kqmap[qkey] = queue

    def find(self, key):
        if key:
            keys = key.split('/')
            cur_key = keys[0]
            if self.kqmap[cur_key]:
                return self.kqmap[cur_key].find('/'.join(keys[1:]))
    def __repr__(self):
        pass
class KFastGroupQueue(KGroupQueueAbstract):
    KQueueCls = KFastQueue
    def __init__(self, *args, **kw):
        super(KFastGroupQueue, self).__init__(*args, **kw)

class KTokenBucketGroupQueue(KGroupQueueAbstract):
    KQueueCls = KTokenBucketQueue
    def __init__(self, rate_limit, capacity=1, *args, **kw):
        super(KTokenBucketGroupQueue, self).__init__(*args, **kw)
        self._bucket = TokenBucket(rate_limit, capacity)
    def get(self, key=None, block=True):
        if not self._bucket.can_consume(1):
            raise RateLimitExceeded()
        return self._get(key, block=block)

class KQueue(object):
    @classmethod
    def make(cls, **kw):
        qkey = kw['qkey']
        kq = KFastQueue(qkey)
        if 'rate_limit' in kw and kw['rate_limit']:
            kw['rate_limit'] = timeutils.rate(kw['rate_limit'])
            kq = KTokenBucketQueue(queue=kq, **kw)
        return kq
class KGroupQueue(object):
    @classmethod
    def make(cls, **kw):
        if 'rate_limit' in kw and kw['rate_limit']:
            kw['rate_limit'] = timeutils.rate(kw['rate_limit'])
            groupqueue = KTokenBucketGroupQueue(**kw)
        else:
            groupqueue = KFastGroupQueue(**kw)
        return groupqueue
