"""
.. module:: requestgraphelement.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from graph.elementtype import VertexType

class RequestElementType(object):
    '''
    The RequestElementType has to be used in order to create a new graph
    element type. It represents a requested hyperspace for graph elements of
    the same type.
    '''

    def __init__(self, element_type, *args):
        self._attr = list(args)
        self._element_type = element_type
        
    def get_structured_attr(self):
        return self._attr
    
    def get_element_type(self):
        return self._element_type
    
class RequestGraphElement(object):
    
    def __init__(self, element_type, properties = {}):
        self._element_type = element_type
        self._type_definition = element_type.get_type_definition()
        self._properties = properties
    
    def add_property(self, key, value):
        '''
        Adds a property to this requested graph element.
        
        Args:
            key: The property's key (str).
            value: The property's value (any type).
        '''
        self._properties[key] = value
    
    def get_structured_attr(self):
        result = {}
        for attr in self._properties:
            if attr in self._type_definition:
                result[attr] = self._properties[attr]
        return result
    
    def get_unstructured_attr(self):
        result = {}
        for attr in self._properties:
            if attr not in self._type_definition:
                result[attr] = self._properties[attr]
        return result
    
class RequestVertexType(RequestElementType, object):
    '''
    The RequestVertexType is used in order to create a new vertex type in the database.
    '''
    
    def __init__(self, type_name, *structured_attr):
        super(RequestVertexType, self).__init__(type_name, *structured_attr)
        
class RequestEdgeType(RequestElementType, object):
    '''
    The RequestEdgeType is used in order to create a new edge type in the database.
    '''
    
    def __init__(self, type_name, *structured_attr):
        super(RequestEdgeType, self).__init__(type_name, *structured_attr) 
        
            
class RequestVertex(RequestGraphElement, object):
    '''
    The RequestVertex is used in order to insert a new vertex to the database.
    '''
    
    def __init__(self, vertex_type, properties = {}):
        if not isinstance(vertex_type, VertexType):
            raise TypeError('Illigal argument exception - must be VertexType')
        super(RequestVertex, self).__init__(vertex_type, properties)
    
    def get_vertex_type(self):
        return self._element_type.get_type_name()
    
    
    
    