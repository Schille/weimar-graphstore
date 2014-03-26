"""
.. module:: testhyperdexsysadmin.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
import unittest
from graphstore.sysadmin import SysAdmin
from hyperdex.client import Client
from hyperdex.admin import Admin as HAdmin
from pydevsrc import pydevd



SYSTEM = 'test_graph_system'
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
        self.hyperdex_client = Client(ADDRESS, PORT)
        self.hyperdex_admin = HAdmin(ADDRESS, PORT)
        self.sysadmin = SysAdmin(ADDRESS, PORT, GRAPH, self.hyperdex_client, self.hyperdex_admin)
        self.sysadmin.validate_database()


    def tearDown(self):
        #self.hyperdex_admin.rm_space(TYPE_DESCRIPTION)
        self.hyperdex_admin.rm_space(SYSTEM)
        #self.hyperdex_admin.rm_space(GENERIC_VERTEX)
        self.hyperdex_admin.rm_space('id')
        self.hyperdex_admin.rm_space(GRAPH + '_id_sys')

    def test_add_vertex_type(self):    
        self.sysadmin.add_vertex_type(vertex_type)
        self.assertIn(vertex_type, self.hyperdex_client.get(SYSTEM, VERTEX_TYPES)['value'])
    
    def test_add_edge_type(self):
        self.sysadmin.add_edge_type(edge_type)
        self.assertIn(edge_type, self.hyperdex_client.get(SYSTEM, EDGE_TYPES)['value'])
    
    def test_rm_vertex_type(self):
        self.test_add_vertex_type()
        self.sysadmin.rm_vertex_type(vertex_type)
        self.assertNotIn(vertex_type, self.hyperdex_client.get(SYSTEM, VERTEX_TYPES)['value'])
    
    def test_rm_edge_type(self):
        self.test_add_edge_type()
        self.sysadmin.rm_edge_type(edge_type)
        self.assertNotIn(edge_type, self.hyperdex_client.get(SYSTEM, EDGE_TYPES)['value'])
        
    def test_is_vertex_type(self):
        self.test_add_vertex_type()
        self.assertEqual(self.sysadmin.is_vertex_type(vertex_type), True)
        self.assertEqual(self.sysadmin.is_vertex_type('bla'), False)

    def test_is_edge_type(self):
        self.test_add_edge_type()
        self.assertEqual(self.sysadmin.is_edge_type(edge_type), True)
        self.assertEqual(self.sysadmin.is_edge_type('bla'), False)
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()