"""
.. module:: testhyperdexgraph.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
import unittest

from graph.hyperdexgraph import HyperDexGraph
from hyperdex.admin import Admin
from hyperdex.client import Client
from graph.requestgraphelements import RequestVertexType, RequestEdgeType,\
    RequestVertex
import os
from pydevsrc import pydevd


SYSTEM = 'test_graph_system'
NEXT_ID = 'nextid'
OBSOLETE_ID = 'obsolete_id'
VERTEX_TYPES = 'vertex_types'
EDGE_TYPES = 'edge_types'
TYPE_DESCRIPTION = 'test_graph_space_description'
GENERIC_VERTEX = 'test_graph_generic_vertex'
ADDRESS = '192.168.100.26'
PORT = 1990
vertex_type = 'testvertex'
edge_type = 'testedge'
GRAPH = 'test_graph'

class Test(unittest.TestCase):


    def setUp(self):
        self.hyperdex = Admin(ADDRESS, PORT)
        self.hyperdex_client = Client(ADDRESS, PORT)
        self.g = HyperDexGraph(ADDRESS, PORT, GRAPH)


    def tearDown(self):
        self.hyperdex.rm_space(TYPE_DESCRIPTION)
        self.hyperdex.rm_space(SYSTEM)
        self.hyperdex.rm_space(GENERIC_VERTEX)
        self.hyperdex.rm_space('id')
        self.hyperdex.rm_space(GRAPH + '_id_sys')
        
        


    def test_create_vertex_type(self):
        req1 = RequestVertexType(vertex_type, ('string', 'name'), ('int', 'age'))
        vertext1 = self.g.create_vertex_type(req1)
        self.assertDictEqual({'age': 'int', 'outgoing_edges': 'map(int, string)', 'incoming_edges': 'map(int, string)', 'name': 'string'},
                             vertext1.get_type_definition())
        self.assertEqual(vertex_type, vertext1.get_type_name())
        os.system('hyperdex wait-until-stable -h {} -p {}'.format(ADDRESS, PORT))
        vertext1.remove()
        
    def test_create_edge_type(self):
        req1 = RequestEdgeType(edge_type,('string', 'name'), ('int', 'age'))
        #pydevd.settrace('192.168.57.1', 5678)
        edge1 = self.g.create_edge_type(req1)
        self.assertEqual({'source_vertex_type': 'string', 'age': 'int', 'name': 'string', 'source_uid': 'int', 'target': 'map(int, string)'},
                          edge1.get_type_definition())
        os.system('hyperdex wait-until-stable -h {} -p {}'.format(ADDRESS, PORT))
        edge1.remove()
        
    def test_get_vertex_type(self):
        req1 = RequestVertexType(vertex_type, ('string', 'name'), ('int', 'age'))
        self.g.create_vertex_type(req1)
        vertext1 = self.g.get_vertex_type(vertex_type)
        self.assertDictEqual({'age': 'int', 'outgoing_edges': 'map(int, string)', 'incoming_edges': 'map(int, string)', 'name': 'string'},
                             vertext1.get_type_definition())
        self.assertEqual(vertex_type, vertext1.get_type_name())
        vertext1.remove()
        
    def test_complex_graph(self):
        pass
        #pydevd.settrace('192.168.57.1', 5678)
        #create vertex type User
        user_type=RequestVertexType('User', ('string', 'first'), ('string', 'last'), ('int', 'age'))
        user=self.g.create_vertex_type(user_type)
        #create vertex type Movie
        movie_type=RequestVertexType('Movie', ('string', 'title'), ('int', 'length'), ('int', 'released'), ('string', 'type'))
        movie=self.g.create_vertex_type(movie_type)
        #create edge type rates
        rates_type=RequestEdgeType('rates', ('int', 'stars'))
        rates=self.g.create_edge_type(rates_type)
        #insert some users
        u1=self.g.insert_vertex(RequestVertex(user, {'first':'Scrooge', 'last':'McDuck', 'age':67}))
        u2=self.g.insert_vertex(RequestVertex(user, {'first':'Donald', 'last':'Duck', 'age':77}))
        u3=self.g.insert_vertex(RequestVertex(user, {'first':'Mikey', 'last':'Mouse', 'age':80}))
        u4=self.g.insert_vertex(RequestVertex(user, {'first':'Gus', 'last':'Goose', 'age':76}))
        u5=self.g.insert_vertex(RequestVertex(user, {'first':'Black', 'last':'Pete', 'age':83}))
        #insert some movies
        m1=self.g.insert_vertex(RequestVertex(movie, {'title':'DuckTails', 'length':22, 'released':1987,'type':'series'}))
        m2=self.g.insert_vertex(RequestVertex(movie, {'title':'The Wise Little Hen', 'length':7, 'released':1934,'type':'film'}))
        m3=self.g.insert_vertex(RequestVertex(movie, {'title':'Mickey\'s Christmas Carol', 'length':25, 'released':1983,'type':'film'}))
        m4=self.g.insert_vertex(RequestVertex(movie, {'title':'Darkwing Duck', 'length':22, 'released':1991,'type':'series'}))
        #attach some movie ratings
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
        
        #count if everything is present
        self.assertEqual(5, user.count())
        self.assertEqual(4, movie.count())
        self.assertEqual(12, rates.count())
        #perform some simple traversals
        self.assertEqual('DuckTails',u1.get_outgoing_edges(rates)[0].get_target()[0].get_property('title'))
        #calc avg rating of DuckTails
        r, c = 0,0
        for rating in m1.get_incoming_edges(rates):
            c = c + 1
            r = r + rating.stars
        self.assertEqual(3, r / c) # Average stars will be 15 / 5 = 3
        #search vertices
        l1=self.g.search_vertex(user, ('last', 'Duck'))
        r = []
        for vertex in l1:
            r.append(vertex.first + ' ' + vertex.last)
        self.assertIn('Donald Duck', r)
        #Reqex search is not yet implemented
        #self.assertIn('Scrooge McDuck', r)
        
        #pydevd.settrace('192.168.57.1', 5678)
        self.assertEqual(3, len(m4.get_incoming_edges()))
        self.assertEqual(3, len(u2.get_outgoing_edges()))
        #remove an item recursively 
        m4.remove()
        self.assertEqual(5, user.count())
        self.assertEqual(3, movie.count())
        self.assertEqual(9, rates.count())
        self.assertEqual(2, len(u2.get_outgoing_edges()))
        #remove yet another item 
        u1.remove()
        #calc avg rating of DuckTails again
        r, c = 0,0
        for rating in m1.get_incoming_edges(rates):
            c = c + 1
            r = r + rating.get_property('stars')
        self.assertEqual(2.5, r / float(c)) # Average stars will be now 10 / 4 = 2.5
        
        #add some unstructured attributes
        m1.set_property('comment', 'DuckTales is an American animated television series produced by Disney Television Animation.')
        #see whether it was stored.
        self.assertItemsEqual(['comment', 'outgoing_edges', 'incoming_edges', 'title',\
                               'released', 'length', 'type'], m1.get_property_keys())
        self.assertEqual('DuckTales is an American animated television series produced by Disney Television Animation.',m1.comment)
        
        
        #remove everything
        user.remove()
        movie.remove()
        rates.remove()

    
    def test_insert_vertex(self):
        user_type=RequestVertexType('User', ('string', 'first'), ('string', 'last'), ('int', 'age'))
        user=self.g.create_vertex_type(user_type)
        u1=self.g.insert_vertex(RequestVertex(user, {'first':'Scrooge', 'last':'McDuck', 'age':67, 'comment': 'Bla Blubb'}))
        u2=self.g.insert_vertex(RequestVertex(user, {'first':'Donald', 'last':'Duck', 'age':77}))
        u3=self.g.insert_vertex(RequestVertex(user, {'first':'Mikey', 'age':80}))
        u4=self.g.insert_vertex(RequestVertex(user, {'first':'Gus'}))
        u5=self.g.insert_vertex(RequestVertex(user, {'first':'Black', 'last':'Pete', 'age':83, 'props': {'attr1': set([1,2,3,4]), 'attr2': [5,6,7,8]}}))
        self.assertIn('comment', u1.get_property_keys())
        self.assertEqual('', u4.last)
        self.assertIsNone(u2.get_property('not_available'))
        self.assertEqual(80, u3.age)
        self.assertEqual({'attr1': set([1,2,3,4]), 'attr2': [5,6,7,8]}, u5.get_property('props'))
        self.assertEqual(5,user.count())
        user.remove()
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()