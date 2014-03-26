import Pyro4
import time
from multiprocessing import Process
from client.requestgraphelements import RequestVertex
from client.elementtype import VertexType
import sys
from client.hyperdexgraph import HyperDexGraph
from client.requestgraphelements import *
import remote.config

def req(server):
    for i in xrange(0,100):
        server.insert_vertex('default', 'Blubb')

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.HMAC_KEY='weimar-graphstore'

g=HyperDexGraph(remote.config.NS_ADDRESS, remote.config.NS_PORT, 'default')

user_type=RequestVertexType('User', ('string', 'first'), ('string', 'last'), ('int', 'age'))
user=g.create_vertex_type(user_type)
#create vertex type Movie
movie_type=RequestVertexType('Movie', ('string', 'title'), ('int', 'length'), ('int', 'released'), ('string', 'type'))
movie=g.create_vertex_type(movie_type)
#create edge type rates
rates_type=RequestEdgeType('rates', ('int', 'stars'))
rates=g.create_edge_type(rates_type)
#insert some users
try:
    print(rates.get_type_definition())
    
    u1=g.insert_vertex(RequestVertex(user, {'first':'Scrooge', 'last':'McDuck', 'age':67}))
    u2=g.insert_vertex(RequestVertex(user, {'first':'Donald', 'last':'Duck', 'age':77}))
    u3=g.insert_vertex(RequestVertex(user, {'first':'Mikey', 'last':'Mouse', 'age':80}))
    u4=g.insert_vertex(RequestVertex(user, {'first':'Gus', 'last':'Goose', 'age':76}))
    u5=g.insert_vertex(RequestVertex(user, {'first':'Black', 'last':'Pete', 'age':83}))
    #insert some movies
    m1=g.insert_vertex(RequestVertex(movie, {'title':'DuckTails', 'length':22, 'released':1987,'type':'series'}))
    m2=g.insert_vertex(RequestVertex(movie, {'title':'The Wise Little Hen', 'length':7, 'released':1934,'type':'film'}))
    m3=g.insert_vertex(RequestVertex(movie, {'title':'Mickey\'s Christmas Carol', 'length':25, 'released':1983,'type':'film'}))
    m4=g.insert_vertex(RequestVertex(movie, {'title':'Darkwing Duck', 'length':22, 'released':1991,'type':'series'}))

    print('Users: ' + str(user.count()))
    print('Movies: ' + str(movie.count()))

    u1.add_edge(m1, rates, {'stars':5})
    u2.add_edge(m1, rates, {'stars':3})
    u2.add_edge(m2, rates, {'stars':1})
    u2.add_edge(m4, rates, {'stars':5})
    u3.add_edge(m2, rates, {'stars':4})
    u3.add_edge(m3, rates, {'stars':5})
    u3.add_edge(m1, rates, {'stars':2})
    u4.add_edge(m1, rates, {'stars':4})
    u4.add_edge(m4, rates, {'stars':5})
    u5.add_edge(m1, rates, {'stars':1})
    u5.add_edge(m3, rates, {'stars':1})
    u5.add_edge(m4, rates, {'stars':2})
    
    print('Done')
    
    print(u1.get_outgoing_edges(rates)[0].get_target()[0].get_property('title'))
    
    r, c = 0,0
    for rating in m1.get_incoming_edges(rates):
        c = c + 1
        r = r + rating.get_property('stars')
    print('avg: ' + str(r/c))
    
    
    m4.remove()
    u1.remove()
    
    print('Users: ' + str(user.count()))
    print('Movies: ' + str(movie.count()))
    
    
    r, c = 0,0
    for rating in m1.get_incoming_edges(rates):
        c = c + 1
        r = r + rating.get_property('stars')
    print('avg: ' + str(r/float(c)))
    
    m1.set_property('comment', 'DuckTales is an American animated television series produced by Disney Television Animation.')
    print(m1.get_property('comment'))
    
    movie.remove()
    user.remove()
    rates.remove()
except Exception, e:
    print(e)
    movie.remove()
    user.remove()
    rates.remove()
'''
start = time.time()
for i in xrange(0,5):
    t=Process(target=req, args=((server),))
    t.start()
t.join()
end = time.time()
print end - start
'''
sys.exit(0)