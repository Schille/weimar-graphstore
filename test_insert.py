import Pyro4
import time
import multiprocessing as mp 
from client.requestgraphelements import RequestVertex
from client.elementtype import VertexType
import sys
import Queue
from client.hyperdexgraph import HyperDexGraph
from client.requestgraphelements import *
import names, random
import remote.config
import time

def req(people):
    Pyro4.config.SERIALIZER = 'pickle'
    Pyro4.config.HMAC_KEY='weimar-graphstore'
    g=HyperDexGraph(remote.config.WEIMAR_ADDRESS_OUTSIDE, remote.config.WEIMAR_PORT_OUTSIDE, 'default')
    while(not people.empty()):
        try:
            p = people.get(False)
            g.insert_vertex(RequestVertex(user, {'first':p[0], 'last':p[1], 'age':p[2]}))
        except Queue.Empty:
            return
            

def printer(people):
    i = 5
    old=people.qsize()
    print('People to be inserted: ' + str(old))
    while(not people.empty()):
        throughput = (old - people.qsize()) / float(i)
        print('Approximate {} insert/s'.format(throughput))
        old=people.qsize()
        time.sleep(i)
        

V_COUNT = 10000
print('Creating People')
people = mp.Queue()
for i in xrange(0, V_COUNT):
    people.put((names.get_first_name(), names.get_last_name(), random.randint(0, 100)))
print('Creating People...Done')


print('Creating VertexType User')
g=HyperDexGraph(remote.config.WEIMAR_ADDRESS_OUTSIDE, remote.config.WEIMAR_PORT_OUTSIDE, 'default')
user_type=RequestVertexType('User', ('string', 'first'), ('string', 'last'), ('int', 'age'))
user=g.create_vertex_type(user_type)
print('Creating VertexType User...Done')




try:
    processes = []
    print('Loading People...')
    k = mp.Process(target=printer, args=((people),))
    k.start()
    start = time.time()
    for i in xrange(0,10):
        p = mp.Process(target=req, args=((people),))
        processes.append(p)
        
    [p.start() for p in processes]
    [p.join() for p in processes]
    end = time.time()
    k.terminate()
    print('== Time consumed: {}s for {} vertices =='.format(end - start, user.count()))
    print('Average insert rate is {}'.format(float(user.count()) / float(end - start)))
    user.remove()
    
except Exception, e:
    print(e)
    user.remove()
    

sys.exit(0)