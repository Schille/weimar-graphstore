"""
.. module:: testelementtype.py
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
from graph.graphelement import Vertex


SYSTEM = 'test_graph_system'
NEXT_ID = 'nextid'
OBSOLETE_ID = 'obsolete_id'
VERTEX_TYPES = 'vertex_types'
EDGE_TYPES = 'edge_types'
TYPE_DESCRIPTION = 'test_graph_space_description'
GENERIC_VERTEX = 'test_graph_generic_vertex'
ADDRESS = '127.0.0.1'
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

    def test_count_elements(self):
        user_req = RequestVertexType('User', ('string', 'name'), ('int', 'age'))
        self.g.create_vertex_type(user_req)
        user = self.g.get_vertex_type('User')
        for i in xrange(50):
            self.g.insert_vertex(RequestVertex(user, {'name':'user'+str(i) , 'age':i}))
        self.assertEqual(50, user.count())
        
        
    def test_iterate_elements(self):
        #pydevd.settrace('192.168.57.1', 5678)
        user_req = RequestVertexType('User', ('string', 'name'), ('int', 'age'))
        self.g.create_vertex_type(user_req)
        user = self.g.get_vertex_type('User')
        all_users = user.get_vertices()
        self.assertEqual(50, len(all_users))
        self.assertTrue(isinstance(all_users[0], Vertex))
        user.remove()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()