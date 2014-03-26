"""
.. module:: hyperdexgraph.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from graphstore.storage import HyperDexStore as HyperDex
from graph.elementtype import VertexType, EdgeType
from graph.graphelement import Vertex
from graphstore.graphexception import ElementNotFoundException, TypeNotFoundException
from graph.requestgraphelements import RequestVertexType, RequestEdgeType,\
    RequestVertex
from serialization import serializer
import logging


class HyperDexGraph(object):
    '''
    This is the basic HyperDexGraph API, which provides for interacting with the underlying
    database. The API is created through HyperDexGraph(ADRESS, PORT) in order to
    connect to the HyperDex coordinator process. The following features are currently available:
    create VertexType - creates a  new type of vertices
    create EdgeType - creates a new type of edges (mandatory)
    get VertexType - returns a type description
    get EdgeType - return a type description
    insert Vertex - adds a new vertex to the graph
    get Vertex - returns a requested graph element
    search - performs a lookup for vertices, edges, graph pattern matching
    '''

    def __init__(self, address, port, graph):
        '''
        Creates a new HyperDexGraph API in order to work with HyperDex as a graph store.
        
        Args:
            address: The IP address of the HyperDex coordinator process (str).
            port: The port number of the HyperDex coordinator process (int).
            graph: The identifier of the graph to be opened (str).
            
        Returns:
            Returns a newly created HyperDexGraph handle for the database.
        '''
        logging.basicConfig(filename='hyperdexgraph.log', filemode='w', level=logging.DEBUG)
        self._storage = HyperDex(address, port, graph)
        self._graph_name = graph
        
    
    def get_vertex_type(self, vertex_type):
        '''
        Retrieves the VertexType representation of a vertex type, which is an entity a vertex
        can belong to.
        
        Args:
            vertex_type: A valid VertexType identifier (str).
             
        Returns:
            A VertexType object containing the type definition and a handle on the group
            of vertices.
        '''
        if(self._storage.vertex_type_exists(vertex_type)):
            logging.info('Retrieving VertexType object for ' + vertex_type)
            return VertexType(self._storage, vertex_type)
        else:
            logging.debug('VertexType object not found: ' + vertex_type)
            raise TypeNotFoundException(vertex_type)
    
    
    def get_edge_type(self, edge_type):
        '''
        Retrieves the EdgeType representation of an edge type, which is an entity an edge
        can belong to.
        
        Args:
            edge_type: An valid EdgeType identifier (str).
             
        Returns:
            An EdgeType object containing the type definition and a handle on the group
            of edges.
        '''
        if(self._storage.edge_type_exists(edge_type)):
            logging.info('Retrieving EdgeType object for ' + edge_type)
            return EdgeType(self._storage, edge_type)
        else:
            logging.debug('EdgeType object not found: ' + edge_type)
            raise TypeNotFoundException(edge_type)
    
    def create_vertex_type(self, requested_vertex_type):
        '''
        Creates a new type of vertex categories. This is used in order to store vertices of a common 
        entity.
        
        Args:
            requested_vertex_type: A RequestVertexType object containing the identifier 
            and type description (RequestVertexType).
             
        Returns:
            A VertexType object containing the type definition and a handle on the group
            of vertices.
        '''
        logging.info('Creating VertexType object for {} in {} '.format(requested_vertex_type.get_element_type(), self._graph_name))
        if isinstance(requested_vertex_type, RequestVertexType):
            self._storage.add_vertex_type(requested_vertex_type.get_element_type(), requested_vertex_type.get_structured_attr())
            return VertexType(self._storage, requested_vertex_type.get_element_type())
        else:
            logging.debug('Creating VertexType was: Illegal argument exception')
            raise TypeError('Illegal argument exception - must be instance of RequestVertexType')
    
    
    def create_edge_type(self, requested_edge_type):
        '''
        Creates a new type of edge categories. This is used in order to store edges of a common 
        entity.
        
        Args:
            requested_edge_type: A RequestEdgeType object containing the identifier 
            and type description (RequestEdgeType).
             
        Returns:
            An EdgeType object containing the type definition and a handle on the group
            of edges.
        '''
        logging.info('Creating EdgeType object for {} in {} '.format(requested_edge_type.get_element_type(), self._graph_name))
        if isinstance(requested_edge_type, RequestEdgeType):
            self._storage.add_edge_type(requested_edge_type.get_element_type(), requested_edge_type.get_structured_attr())
            return EdgeType(self._storage, requested_edge_type.get_element_type())
        else:
            logging.debug('Creating EdgeType was: Illegal argument exception')
            raise TypeError('Illegal argument exception - must be instance of RequestEdgeType')
    

    def insert_vertex(self, requested_vertex):
        '''
        Inserts a vertex of a given type into the database. The vertex must be represented as RequestVertex
        in order to complete successfully.
        
        Args:
            requested_vertex: A non-persistent representation of the vertex intended to be inserted into the 
            database.
             
        Returns: 
            A Vertex object, representing a reference to the related object within the database. This object can be 
            used to modify or delete the vertex from the graph. 
        '''
        if isinstance(requested_vertex, RequestVertex):
            logging.info('Inserting a vertex was requested into ' + self._graph_name)
            uid = self._storage.add_vertex(requested_vertex.get_vertex_type(), requested_vertex.get_structured_attr(),
                                      serializer.serialize(requested_vertex.get_unstructured_attr()))
            return Vertex(uid, requested_vertex.get_vertex_type(), self._storage)
        else:
            logging.debug('Inserting vertex was: Illegal argument exception')
            raise TypeError('Illegal argument exception - must be instance of RequestVertex')
    
    def get_vertex(self, uid, vertex_type):
        #TODO perform a lookup in all spaces, in case there is no vertex_type specified
        '''
        Retrieves a vertex from the graph.
        
        Args:
            uid: The unique identifier of the vertex (int). 
            vertex_type: The type (identifier) of the requested vertex (str/VertexType).
             
        Returns: 
            A Vertex object, representing a reference to the related object within the database. This object can be 
            used to modify or delete the vertex from the graph. 
        '''
        if self._storage.get_graph_element(uid, vertex_type) is not None:
            return Vertex(uid, vertex_type, self._storage)
        else:
            logging.debug('Vertex with type: {} id: {} was not found'.format(str(uid), str(vertex_type)))
            raise ElementNotFoundException(uid, vertex_type)
    
    def search_vertex(self, vertex_type, *search):
        #TODO Improve search, also to match graph pattern
        #TODO create code documentation 
        query = {}
        result = []
        for stmt in search:
            query[stmt[0]] = stmt[1]
        if isinstance(vertex_type, VertexType):
            vertices = self._storage.search(vertex_type.get_type_name(), query)
        elif isinstance(vertex_type, str):
            vertices = self._storage.search(vertex_type, query)
        else:
            raise TypeError('Illegal argument exception.')
        for vertex in vertices:
            result.append(Vertex(vertex['graph_uid'], vertex_type, self._storage))
        return result 
    
