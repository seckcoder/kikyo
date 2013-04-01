import kikyo
from kikyo import KQue
kikyo.monkey_patch(socket=True)
mykikyo = kikyo.Kikyo()

urgent_que = GroupQue(qkey="urgent", rate_limit="200/s")
kque1 = KQue(qkey="10.1.9.9", rate_limit="100/s")
kque2 = KQue(qkey="10.1.9.10", rate_limit="200/s")
urgent_que.add(kque1, kque2)
normal_que = GroupQue(qkey="normal", rate_limit="400/s")
normal_que.add(kque1, kque2)

mykikyo.addque(urget_que,, normal_que)

@mykikyo.async(qkey="urgent/10.1.9.9")
def foo(arg):
    print "foo"

@mykikyo.async(rate_limit="xxx")  # add a queue for this task type
def foo2(arg):
    print "foo"
def bar(arg):
    print "bar"

@mykikyo.beat()
def beat():
    print "beat"
class Study(object):
    @mykikyo.async
    def foo(self):
        pass
foo(link_value=bar,
    link_exception=exception)

sdk.statuses_show = kikyo.async(sdk.statuses_show)
def handle_req(req):
    def callback(results):
        pass
    sdk.statuses_show(link=callback, qkey="urgent/10.1.9.9;")
    sdk.statuses_show(link=callback)  # no qkey specified, use cycling
