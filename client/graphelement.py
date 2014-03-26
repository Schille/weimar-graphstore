"""
.. module:: graphelement.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from client.elementtype import EdgeType
GENERIC_VERTEX = 'generic_vertex'
from client import elementtype


class GraphElement(object):
    '''
    This class is the in-memory representation of an existing graph element. It is a handle
    on a certain vertex or edge object, which provides for modifying this particular element. 
    '''


    def __init__(self, uid, element_type, element_svr, graph_name):
        '''
        Constructor
        '''
        self._uid = uid
        self._graphname = graph_name
        self._element_svr = element_svr
        if isinstance(element_type, elementtype.ElementType): #if element_type is a complex type object
            self._element_type = element_type.get_type_name()
        elif isinstance(element_type, str):#if element_type is merely the identifier
            self._element_type = element_type
        else:
            raise TypeError('Illigal argument exception')

    '''        
    def __getattr__(self, attr):
        # enable property access directly by [graphelement].[propertykey]
        pass
    
    def __setattr__(self, attr, value):
        pass

    def _get_attr_value(self, key):
        pass 
    '''
    def get_property(self, key):
        '''
        Returns the value for the requested property key.
        
        Args:
            key: The property identifier (str).
        
        Return:
            The value of the requested property (any type).
        '''
        #retrieve attributes for this very graph element, this is lazy
        return self._element_svr.get_property(self._graphname, self._uid, self._element_type, key)
        
    def set_property(self, key, value):
        '''
        Sets a value for a certain key on this very graph element.
        
        Args:
            key: The property identifier (str). 
            value: The value of this property (any type).
        '''
        self._element_svr.set_property(self._graphname, self._uid, self._element_type, key, value)

    
    def get_property_keys(self):
        '''
        Returns all keys available for this graph element.
        
        Return:
            Set of available keys (set<str>).
        '''
        return self._element_svr.get_property_keys(self._graphname, self._uid, self._element_type)
        

class Vertex(GraphElement):
    
    def __init__(self, uid, vertex_type, element_svr, graph_name):
        if vertex_type is None:
            vertex_type = GENERIC_VERTEX
        super(Vertex, self).__init__(uid,vertex_type, element_svr, graph_name)
    
    def add_edge(self, target_vertex, edge_type, struct_attr = {}, unstruc_attr = {}):
        '''
        Adds an edge to an existing vertex object. This edge can point to multiple 
        target vertices.
        
        Args:
            target_vertices: A target vertex/list of target vertices of this edge (Vertex/list<Vertex>).
            edge_type: The EdgeType of the intended edge (EdgeType).
            struct_attr: The structured attributes of this edge object (dict).
            unstruct_attr: The unstructured attributes of this edge object (dict).
        
        Return:
            An edge object, representing the newly created edge between the given vertices (Edge). 
        '''
        uid = self._element_svr.v_add_edge(self._graphname, self._uid, self._element_type.split(':',1)[1], target_vertex._element_type.split(':',1)[1],\
                                             target_vertex._uid, edge_type.get_type_name().split(':',1)[1], struct_attr, unstruc_attr)
        return Edge(uid, edge_type, self._element_svr, self._graphname)
    
    def rm_edge(self, edge):
        self._element_svr.v_rm_edge(self._graphname, self._uid, self._element_type, edge._uid, edge._element_type)
    
    def get_outgoing_edges(self, edge_type = None):
        if(isinstance(edge_type, EdgeType)):
            edge_type = edge_type.get_type_name().split(':', 1)[1]
        outgoing = self._element_svr.v_get_outgoing_edges(self._graphname, self._uid, self._element_type.split(':', 1)[1], edge_type)
        result = []
        for k in outgoing:
            result.append(Edge(k, 'edge:'+outgoing[k], self._element_svr, self._graphname))
        return result
    
    def get_incoming_edges(self, edge_type = None):
        if(isinstance(edge_type, EdgeType)):
            edge_type = edge_type.get_type_name().split(':', 1)[1]
        incoming = self._element_svr.v_get_incoming_edges(self._graphname, self._uid, self._element_type.split(':', 1)[1], edge_type)
        result = []
        for k in incoming:
            result.append(Edge(k, 'edge:'+incoming[k], self._element_svr, self._graphname))
        return result
        
    def remove(self):
        self._element_svr.remove(self._graphname, self._uid, self._element_type)


class Edge(GraphElement):
    
    def __init__(self, uid, edge_type,  element_svr, graph_name):
        super(Edge, self).__init__(uid, edge_type, element_svr, graph_name)
        self._source = None
    
    
    def get_target(self):
        result = []
        vertices = self._element_svr.e_get_target(self._graphname, self._uid, self._element_type.split(':',1)[1])
        for v in vertices:
            result.append(Vertex(v, 'vertex:' + vertices[v], self._element_svr, self._graphname))
        return result
    
    def get_source(self):
        v = self._element_svr.e_get_source(self._graphname, self._uid, self._element_type.split(':',1)[1])
        return Vertex(v[0], 'vertex:' + v[1], self._element_svr, self._graphname)
    
    def add_target(self, target_vertex):  
        self._element_svr.e_add_target(self._graphname, self._uid, self._element_type.split(':',1)[1], target_vertex._uid, target_vertex._element_type.split(':',1)[1])    
    
    def remove_target(self, target_vertex):
        self._element_svr.e_rm_target(self._graphname, self._uid, self._element_type.split(':',1)[1], target_vertex._uid, target_vertex._element_type.split(':',1)[1])
    
    def remove(self):
        self._element_svr.remove(self._graphname, self._uid, self._element_type)

            
    
    
    
    
    
    
    
        
        
        
        
