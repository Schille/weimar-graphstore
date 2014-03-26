"""
.. module:: elementtype.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""

import logging


class ElementType(object):
    '''
    The ElementType class is an in-memory representation of a graph element
    type. It provides some functions to operate on all entities of the same type
    and keeps the description of the structured attributes.
    '''


    def __init__(self, type_svr, type_name, graph_name):
        self._type_svr = type_svr
        self._typename = type_name
        self._graphname = graph_name


    def get_type_definition(self):
        '''
        Returns a dictionary comprising the structured attributes of 
        this graph element type.
        
        Return:
            The created type declaration (dict).
        '''
        return self._type_svr.get_type_definition(self._graphname, self._typename)
    
    
    def get_type_name(self):
        '''
        Returns the type name of this object.
        
        Return:
            The type name (str).
        '''
        return self._typename
    
        
    def count(self):
        '''
        Returns the number of graph elements associated with this type.
        
        Return:
            Count of related graph elements (int).
        '''
        return self._type_svr.count(self._graphname, self._typename)
        
    
    #TDOD provide search method on these elements
    
    
class VertexType(ElementType, object):
    '''
    The VertexType.
    '''

    def __init__(self, type_svr, vertex_type, graph_name):
        vertex_type = 'vertex:' + vertex_type
        super(VertexType, self).__init__(type_svr, vertex_type, graph_name)
        
    def get_vertices(self):
        pass
    
    def remove(self):
        '''
        Removes this element type and all associated elements.
        '''
        self._type_svr.remove(self._graphname, self._typename)
        
class EdgeType(ElementType, object):
    '''
    The EdgeType.
    '''

    def __init__(self, type_svr, edge_type, graph_name):
        edge_type = 'edge:' + edge_type
        super(EdgeType, self).__init__(type_svr, edge_type, graph_name)
    
    def get_edges(self):
        pass
        
    def remove(self):
        '''
        Removes this element type and all associated elements.
        '''
        self._type_svr.remove(self._graphname, self._typename)
