import sys
import somata

print 'helo'

def sayHi(name, cb):
    response = "hi to %s" % name
    print response
    cb(response)

test = somata.Service('synthesis', {'sayHi': sayHi}, {'bind_port': 5555})
