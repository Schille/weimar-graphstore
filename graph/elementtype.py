"""
.. module:: elementtype.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
class ElementType(object):
    '''
    The ElementType class is an in-memory representation of a graph element
    type. It provides some functions to operate on all entities of the same type
    and keeps the description of the structured attributes.
    '''


    def __init__(self, typedef, type_name):
        self._get_typedefinition = typedef
        self._typename = type_name


    def get_type_definition(self):
        '''
        Returns a dictionary comprising the structured attributes of 
        this graph element type.
        
        Return:
            The created type declaration (dict).
        '''
        result = {}
        for attr in self._get_typedefinition(self._typename):
            result[attr[1]] = attr[0]
        return result
    
    def get_type_name(self):
        '''
        Returns the type name of this object.
        
        Return:
            The type name (str).
        '''
        return self._typename
    
    
class VertexType(ElementType, object):
    '''
    The VertexType.
    '''

    def __init__(self, typedef, vertex_type):
        super(VertexType, self).__init__(typedef, vertex_type)
        
class EdgeType(ElementType, object):
    '''
    The EdgeType.
    '''

    def __init__(self, typedef, edge_type):
        super(EdgeType, self).__init__(typedef, edge_type)
        