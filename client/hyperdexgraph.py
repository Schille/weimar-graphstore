"""
.. module:: hyperdexgraph.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
import Pyro4
from client.elementtype import VertexType, EdgeType
from client.graphelement import Vertex


class HyperDexGraph(object):
    '''

    '''

    def __init__(self, address, port, graph_name):
        self._graphname = graph_name
        self._weimar_ip = address
        self._port = port
        self._ns = Pyro4.naming.locateNS(address, port)
        self._api = Pyro4.Proxy(self._ns.lookup('weimar.server.api'))
        self._element_type_svr = Pyro4.Proxy(self._ns.lookup('weimar.server.type'))
        self._graph_element_svr = Pyro4.Proxy(self._ns.lookup('weimar.server.element'))
        
        
    def get_vertex_type(self, vertex_type):
        if(self._api.get_vertex_type(self._graphname, vertex_type)):
            return VertexType(self._element_type_svr, vertex_type)
        else:
            #logging.debug('VertexType object not found: ' + vertex_type)
            raise Exception(vertex_type)
       
    def get_edge_type(self, edge_type):
        if(self._api.get_edge_type(self._graphname, edge_type)):
            return EdgeType(self._element_type_svr, edge_type)
        else:
            #logging.debug('VertexType object not found: ' + vertex_type)
            raise Exception(edge_type)
    
    def create_vertex_type(self, requested_vertex_type):
        if(self._api.create_vertex_type(self._graphname, requested_vertex_type.get_element_type(), requested_vertex_type.get_structured_attr())):
            return VertexType(self._element_type_svr, requested_vertex_type.get_element_type(), self._graphname)
        else:
            #TODO duplicated type definition?
            raise Exception('Type was not created.')
        
    def create_edge_type(self, requested_edge_type):
        if(self._api.create_edge_type(self._graphname, requested_edge_type.get_element_type(),requested_edge_type.get_structured_attr())):
            return EdgeType(self._element_type_svr, requested_edge_type.get_element_type(), self._graphname)
        else:
            #TODO duplicated type definition?
            raise Exception('Type was not created.')
    
    def insert_vertex(self, requested_vertex):
        uid = self._api.insert_vertex(self._graphname, requested_vertex.get_vertex_type(), requested_vertex._properties)
        if(uid is not None):
            return Vertex(uid, requested_vertex.get_vertex_type(), self._graph_element_svr, self._graphname)
        raise Exception('TODO')
    
    def get_vertex(self, uid, vertex_type):
        if(self._api.get_vertex(self._graphname, uid, vertex_type.get_type_name())):
            return Vertex(uid, vertex_type.get_type_name(), self._graph_element_svr, self._graphname)
    
    def search_vertex(self, vertex_type, *search):
        #TODO
        pass
    
