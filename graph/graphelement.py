"""
.. module:: graphelement.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from serialization import serializer
from graph import elementtype 
GENERIC_VERTEX = 'generic_vertex'

class GraphElement(object):
    '''
    This class is the in-memory representation of an existing graph element. It is a handle
    on a certain vertex or edge object, which provides for modifying this particular element. 
    '''


    def __init__(self, uid, element_type, storage):
        '''
        Constructor
        '''
        self._uid = uid
        if isinstance(element_type, elementtype.ElementType): #if element_type is a complex type object
            self._element_type = element_type.get_type_name()
        elif isinstance(element_type, str):#if element_type is merely the identifier
            self._element_type = element_type
        else:
            raise TypeError('Illigal argument exception')
        self._storage = storage#pass a handle on the underlying graphstorage provider

    def get_property(self, key):
        '''
        Returns the value for the requested property key.
        
        Args:
            key: The property identifier (str).
        
        Return:
            The value of the requested property (any type).
        '''
        #retrieve attributes for this very graph element, this is lazy
        props = self._storage.get_graph_element(self._uid, self._element_type)
        #check whether this key is a structured attributes
        if key in props:
            return props[key]
        else:
            #if the key is not in structured attributes, perform a lookup in the unstructured part
            props = serializer.deserialize(props['value'])#deserialize the binary string
            if key in props:
                return props[key]
            else:
                return None#if not found, return the NoneType

    def set_property(self, key, value):
        '''
        Sets a value for a certain key on this very graph element.
        
        Args:
            key: The property identifier (str). 
            value: The value of this property (any type).
        '''
        #TODO prevent user from changing outgoing/incoming edges
        props = self._storage.get_graph_element(self._uid, self._element_type)
        if key in props: #if the key is part of the structured attributes
            #TODO check type, prevent from storing wrong data types
            props[key] = value
        else: #if the key is not part of the structured attributes, store it as unstructured
            unstr_props = serializer.deserialize(props['value']) #deserialize all unstructured attributes
            unstr_props[key] = value #add the newly created property
            props['value'] = serializer.serialize(unstr_props) #store resulting unstructured attributes, serialize it
        self._storage.put_graph_element(self._uid, self._element_type, props) #put it back to the database
    
    def get_property_keys(self):
        '''
        Returns all keys available for this graph element.
        
        Return:
            Set of available keys (set<str>).
        '''
        props = self._storage.get_graph_element(self._uid, self._element_type)
        result = set()
        for prop in props.keys():
            if prop == 'value':
                unstruct = serializer.deserialize(props[prop])
                for un_prop in unstruct.keys():
                    result.add(un_prop)
                continue
            else:
                result.add(prop)
        return result
        

class Vertex(GraphElement, object):
    
    def __init__(self, uid, vertex_type, storage):
        if vertex_type is None:
            vertex_type = GENERIC_VERTEX
        super(Vertex, self).__init__(uid,vertex_type, storage)
    
    def add_edge(self, target_vertices, edge_type, struct_attr = {}, unstruc_attr = {}):
        if isinstance(edge_type, elementtype.EdgeType):
            edge_type = edge_type.get_type_name()
        if isinstance(target_vertices, list):
            target = {}
            for i in target_vertices:
                target[i._uid] = i._element_type
        else:
            target = {target_vertices._uid : target_vertices._element_type}
        struct_attr['source_uid'] = self._uid
        struct_attr['source_vertex_type'] = self._element_type
        struct_attr['target'] = target
        if(len(unstruc_attr) != 0):
            struct_attr['value'] = serializer.serialize(unstruc_attr)
        edge_uid = self._storage.add_edge(self._uid, self._element_type, target , edge_type, struct_attr)
        return Edge(edge_uid, edge_type, self._storage)
    
    def rm_edge(self, edge):
        self._storage.rm_edge(self._uid, self._element_type, edge._uid, edge._element_type)
        del edge
    
    def get_outgoing_edges(self, edge_type = None):
        result = []
        edges = self.get_property('outgoing_edges')
        if edge_type is None:
            for edge in edges.keys():
                result.append(Edge(edge, edges[edge], self._storage))
        else:
            if isinstance(edge_type, elementtype.EdgeType):
                edge_type = edge_type.get_type_name()
            for edge in edges.keys():
                if edges[edge] == edge_type:
                    result.append(Edge(edge, edges[edge], self._storage))
        return result
    
    def get_incoming_edges(self, edge_type = None):
        result = []
        edges = self.get_property('incoming_edges')
        if edge_type is None:
            for edge in edges.keys():
                result.append(Edge(edge, edges[edge], self._storage))
        else:
            if isinstance(edge_type, elementtype.EdgeType):
                edge_type = edge_type.get_type_name()
            for edge in edges.keys():
                if edges[edge] == edge_type:
                    result.append(Edge(edge, edges[edge], self._storage))
        return result
        
    def remove(self):
        for edge in self.get_incoming_edges():
            edge.remove()
        for edge in self.get_outgoing_edges():
            edge.remove()
        self._storage.rm_vertex(self._uid, self._element_type)


class Edge(GraphElement):
    
    def __init__(self, uid, edge_type,  storage):
        super(Edge, self).__init__(uid, edge_type, storage)
        self._source = None
    
    
    def get_target(self):
        result = []
        vertices = self.get_property('target')
        for vertex in vertices.keys():
            result.append(Vertex(vertex, vertices[vertex], self._storage))
        return result
    
    def get_source(self):
        if self._source is None:
            vertex_uid = self.get_property('source_uid')
            vertex_type = self.get_property('source_vertex_type')
            self._source = Vertex(vertex_uid, vertex_type, self._storage)
        return self._source
    
    def remove_target(self, target_vertices):
        if isinstance(target_vertices, list):
            for vertex in target_vertices:
                self.remove(vertex)
        else:
            self._storage.edge_rm_target(target_vertices._uid, target_vertices._element_type, self._uid, self._element_type)
        return True
    
    def remove(self):
        self._storage.rm_edge(self.get_source()._uid, self._source._element_type, self._uid, self._element_type)
        return True
    
    def add_target(self, target_vertices):  
        if isinstance(target_vertices, list):
            for vertex in target_vertices:
                self.add_target(vertex)
        else:
            self._storage.edge_add_target(target_vertices._uid, target_vertices._element_type, self._uid, self._element_type)
        return True
            
    
    
    
    
    
    
    
        
        
        
        
