from kikyo.worker.kqueue import KQueue, KGroupQueue, RateLimitExceeded
from kikyo.five import Empty
import unittest2

class KQueueTestCase(unittest2.TestCase):
    #@unittest2.skip('test_kqueue')
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
    #@unittest2.skip("test_kqueue_rate_limit")
    def test_kqueue_rate_limit(self):
        kq = KQueue.make(qkey='normal',
                         rate_limit='10/s')
        for i in xrange(1000):
            kq.put(i)

        with self.assertRaises(RateLimitExceeded):
            for i in xrange(10):
                kq.get(block=True)
    def test_kgroupqueue(self):
        kq = KGroupQueue.make(qkey='global')
        kq.touch('normal/10.1.9.9', ['100/s', '200/s'])
        kq.makegroup('urgent/group1')
        kq.touch('urgent/group1/subgroup2/x.x.x.x', priority=3)
        self.assertEqual(kq.find('urgent').priority, 3)
        self.assertEqual(len(kq.queues), 2)
        kq.find('urgent').priority = 2
        self.assertEqual(kq.find('urgent').priority, 2)

    def test_kgroupqueue_ratelimit(self):
        kq = KGroupQueue.make(qkey='global')
        kq.touch('normal/10.1.9.9', [None, '100/s'])
        for i in xrange(200):
            kq.put(i)

        # This will take 2 seconds to finish
        for i in xrange(200):
            kq.get()

        for i in xrange(200):
            kq.put(i)

        with self.assertRaises(Empty):
            for i in xrange(200):
                kq.get(block=False)

    def test_kgroupqueue_priority(self):
        kq = KGroupQueue.make(qkey='global')
        kq.touch('urgent', priority=1)
        kq.touch('normal', priority=2)
        for i in xrange(10):
            kq.put('normal', 'normal')

        kq.put('urgent', 'urgent')
        self.assertEqual(kq.get(), 'urgent')
