"""
.. module:: testhyperdextypeadmin.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
import unittest
from graphstore.typeadmin import TypeAdmin
from hyperdex.client import Client
from hyperdex.admin import Admin



ADDRESS = '127.0.0.1'
PORT = 1990

TYPE_DESCRIPTION = 'space_description'

vertex_type = 'testvertex'
edge_type = 'testedge'

class Test(unittest.TestCase):


    def setUp(self):
        self.hyperdex_client = Client(ADDRESS, PORT)
        self.hyperdex_admin = Admin(ADDRESS, PORT)
        self.typeadmin = TypeAdmin(ADDRESS, PORT,self.hyperdex_client, self.hyperdex_admin)
        self.typeadmin.validate_database()


    def tearDown(self):
        self.hyperdex_admin.rm_space(TYPE_DESCRIPTION)
        


    def test___stringify_attributes(self):
        attributes  = [('string', 'name'), ('int', 'age')]
        result = self.typeadmin._stringify_attributes(attributes)
        self.assertEqual(' string name,\n int age', result)
        
    def test__dictify_attributes(self):
        attributes  = [('string', 'name'), ('int', 'age')]
        result = self.typeadmin._dictify_attributes(attributes)
        self.assertDictEqual({'name':'string', 'age':'int'}, result)
        
    def test_create_element_type(self):
        attributes  = [('string', 'name'), ('int', 'age')]
        self.typeadmin.create_element_type(vertex_type, attributes)
        self.hyperdex_admin.rm_space(vertex_type)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()