from kikyo.worker.kqueue import KQueue, KGroupQueue, RateLimitExceeded
from kikyo.five import Empty
import unittest2
class KQueueTestCase(unittest2.TestCase):
    def test_kqueue(self):
        kq = KQueue.make(qkey='normal')
        kq.put(3)
        kq.put("haha")
        kq.put(KQueue)
        kq.get()
        kq.get()
        kq.get()
        self.assertEqual(kq.qsize(), 0)
        with self.assertRaises(Empty):
            kq.get(block=False)
    def test_kqueue_rate_limit(self):
        kq = KQueue.make(qkey='normal',
                         rate_limit='10/s')
        for i in xrange(1000):
            kq.put(i)

        with self.assertRaises(RateLimitExceeded):
            for i in xrange(10):
                kq.get(block=True)
    def test_kgroupqueue(self):
        kq = KGroupQueue.make()
        for key in ['normal', 'urgent']:
            kq.addque(key, KGroupQueue.make(qkey=key))
