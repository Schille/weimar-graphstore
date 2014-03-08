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


    def __init__(self, storage, type_name):
        self._storage = storage
        self._typename = type_name


    def get_type_definition(self):
        '''
        Returns a dictionary comprising the structured attributes of 
        this graph element type.
        
        Return:
            The created type declaration (dict).
        '''
        result = {}
        for attr in self._storage.typeadmin.get_type_description(self._typename):
            result[attr[1]] = attr[0]
        return result
    
    def get_type_name(self):
        '''
        Returns the type name of this object.
        
        Return:
            The type name (str).
        '''
        return self._typename
    
    def remove(self):
        '''
        Removes this element type and all associated elements.
        '''
        logging.info('Removing ElementType {} and all its entities'.format(self._typename))
        self._storage.rm_vertex_type(self._typename)
        
    def count(self):
        '''
        Returns the number of graph elements associated with this type.
        
        Return:
            Count of related graph elements (int).
        '''
        return self._storage.count_elements(self._typename)
        
    
    #TDOD provide search method on these elements
    
    
class VertexType(ElementType, object):
    '''
    The VertexType.
    '''

    def __init__(self, storage, vertex_type):
        super(VertexType, self).__init__(storage, vertex_type)
        
    def get_vertices(self):
        from graph import graphelement
        return [graphelement.Vertex(v['id'],self._typename,self._storage) for v in self._storage.get_all_elements(self._typename)]
        
class EdgeType(ElementType, object):
    '''
    The EdgeType.
    '''

    def __init__(self, storage, edge_type):
        super(EdgeType, self).__init__(storage, edge_type)
    
    def get_edges(self):
        from graph import graphelement
        return [graphelement.Edge(e['id'],self._typename,self._storage) for e in self._storage.get_all_elements(self._typename)]
        
