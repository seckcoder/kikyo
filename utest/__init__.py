import unittest

from utest.worker.test_kqueue import KQueueTestCase


def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(KQueueTestCase))
    return suite

