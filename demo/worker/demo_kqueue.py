#-*- coding=utf-8 -*-
from kikyo.worker.kqueue import KGroupQueue, KQueue

kq = KGroupQueue.make()
kq.addque('normal', KGroupQueue.make())
kq.addque('urgent', KGroupQueue.make())
kq.find('normal').addque('10.1.9.9', KQueue.make(key='10.1.9.9'))
