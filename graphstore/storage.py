"""
.. module:: storage.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from hyperdex.client import Client
from hyperdex.admin import Admin
from sysadmin import SysAdmin
from typeadmin import TypeAdmin
from hyperdex.client import HyperClientException
from graphexception import ElementNotFoundException,TypeNotFoundException
import os
import logging



class HyperDexStore():
    '''
    The HyperDexStore class provides the first level of abstraction towards the client. It
    wraps the HyperDex python language binding. It is used by the HyperDexGraph API and 
    handles the underlying connection to the database.
    '''

    def __init__(self, address, port, graph_name):
        '''
        Constructor.
        
        Args:
            address: The IP address of HyperDex's coordinator process (str).
            port: The port number of HyperDex's coordinator process (int).
        
        Returns:
            An instance of the HyperDexStore, which wraps the underlying connection.
        '''
        self._address = address
        self._port = port
        self._graph_name = graph_name
        self.hyperdex_client = Client(address, port)#HyperDex python binding (Client)
        self.hyperdex_admin = Admin(address, port)#HyperDex python binding (Admin)
        self.sysadmin = SysAdmin(address, port, graph_name,  self.hyperdex_client, self.hyperdex_admin)
        self.typeadmin = TypeAdmin(address, port, graph_name, self.hyperdex_client, self.hyperdex_admin)
        #validate the underlying database for the particular components, i.e.
        #checking for the system/description space
        self.sysadmin.validate_database()
        self.typeadmin.validate_database()
        
        self._generic_vertex = graph_name + '_generic_vertex'
        self.validate_database()
        
    def validate_database(self):
        try:
            self.hyperdex_client.get(self._generic_vertex, 1)
            logging.info('Generic vertices available')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                logging.info('Creating space generic_vertex')
                self.hyperdex_admin.add_space('''
                space '''+ self._generic_vertex + '''
                key int id
                attributes 
                   map(int, string) incoming_edges,
                   map(int, string) outgoing_edges,
                   string value
                ''')
                os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                return
            logging.error(str(e))
    
    def add_vertex_type(self, vertex_type, attributes):
        '''
        Adds a new vertex type to the database.
        
        Args:
            vertex_type: The identifier of the requested vertex type (str).
            attributes: The schema description of the requested type definition (dict)
        '''
        if attributes is None:
            attributes = []# create empty definition
        #add special attributes for incoming and outgoing edges  
        if ('map(int, string)', 'incoming_edges') not in attributes:
            attributes.append(('map(int, string)', 'incoming_edges'))
        if ('map(int, string)', 'outgoing_edges') not in attributes:
            attributes.append(('map(int, string)', 'outgoing_edges'))
        #create the vertex within the typeadmin
        self.typeadmin.create_element_type(vertex_type, attributes)
        #introduce the type to sysadmin
        self.sysadmin.add_vertex_type(vertex_type)
        logging.debug(self.typeadmin.get_type_description(vertex_type))
    
    def rm_vertex_type(self, vertex_type):
        '''
        Removes the vertex type and all the vertices of this type.
        
        Args:
            vertex_type: The identifier of the requested vertex type (str).
        '''
        self.typeadmin.remove_element_type(vertex_type)
        self.sysadmin.rm_vertex_type(vertex_type)
    
    def rm_edge_type(self, edge_type):
        '''
        Removes the edge type and all the edges of this type.
        
        Args:
            edge_type: The identifier of the requested edge type (str).
        '''
        self.typeadmin.remove_element_type(edge_type)
        self.sysadmin.rm_edge_type(edge_type)
    
    def add_edge_type(self, edge_type, attributes):
        '''
        Adds a new edge type to the database.
        
        Args:
            edge_type: The identifier of the requested edge type (str).
            attributes: The schema description of the requested type definition (dict)
        '''
        if attributes is None:
            attributes = []
        #add special attributes for edge handling
        if ('int', 'source_uid') not in attributes:
            attributes.append(('int', 'source_uid'))
        if ('string', 'source_vertex_type') not in attributes:
            attributes.append(('string', 'source_vertex_type'))
        if ('map(int, string)', 'target') not in attributes:
            attributes.append(('map(int, string)', 'target'))
        #introduce the type to sysadmin
        self.typeadmin.create_element_type(edge_type, attributes)
        self.sysadmin.add_edge_type(edge_type)
        
    def add_vertex(self, vertex_type=None, struct_attr=None, unstrc_att=None):
        '''
        Adds a new vertex to the database and processes the data.
        
        Args:
            vertex_type: The vertex type identifier of the given vertex (str).
            struct_attr: The structured attributes of this vertex, which have been 
                         described in the type definition (dict).
            unstruct_attr: Additional properties of this vertex, not defined in the type
                           definition - already serialized (str).
        Return:
            The assigned unique id for this vertex (int).
        '''
        if vertex_type is None:
            #get the next available id 
            uid = self.sysadmin.get_next_id()
            if struct_attr is None:
                #this vertex has no specified type and is stored as generic vertex
                #besides, this vertex is empty, hence there is no processing necessary
                self.hyperdex_client.put(self._generic_vertex, uid, { 'value' : str(unstrc_att),
                                                         'incoming_edges' : {}, 'outgoing_edges' : {} })
            else:
                #the vertex already got some payload to store
                struct_attr['value'] = str(unstrc_att)
                #there are no edges on newly created vertices
                struct_attr['incoming_edges'] = {} if not 'incoming_edges' in struct_attr else struct_attr['incoming_edges']
                struct_attr['outgoing_edges'] = {} if not 'outgoing_edges' in struct_attr else struct_attr['outgoing_edges']
                self.hyperdex_client.put(self._generic_vertex, uid, struct_attr)
        else:
            #the vertex is of a certain type
            if self.sysadmin.is_vertex_type(vertex_type):
                #get the next available id
                uid = self.sysadmin.get_next_id()
                logging.debug('Inserting vertex for id: ' + str(uid))
                if struct_attr is None:
                    #the vertex has no structured attributes and may empty (except for the unstructured attributes)
                    self.hyperdex_client.put('{}_{}'.format(self._graph_name,vertex_type), uid, { 'value' : str(unstrc_att),
                                                    'incoming_edges' : {}, 'outgoing_edges' : {} })
                else:
                    #set the payload of this vertex
                    struct_attr['value'] = str(unstrc_att)
                    struct_attr['incoming_edges'] = {} if not 'incoming_edges' in struct_attr else struct_attr['incoming_edges']
                    struct_attr['outgoing_edges'] = {} if not 'outgoing_edges' in struct_attr else struct_attr['outgoing_edges']
                    self.hyperdex_client.put('{}_{}'.format(self._graph_name,vertex_type), uid, struct_attr)
            else:
                logging.debug('Vertex type not found: {}_{}'.format(self._graph_name,vertex_type))
                raise TypeNotFoundException('Vertex type not found: {}_{}'.format(self._graph_name, vertex_type))
        return uid

  
    
    def rm_vertex(self, uid, vertex_type=None):
        '''
        Removes a vertex from the database.
        
        Args:
            uid: The numeric identifier of a vertex object (int).
            vertex_type: The type identifier of this vertex (str). 
        '''
        if vertex_type is None:
            self.hyperdex_client.delete(self._generic_vertex, uid)
        else:
            self.hyperdex_client.delete('{}_{}'.format(self._graph_name,vertex_type), uid)
        self.sysadmin.add_obsolete_id(uid)    
    
    def get_graph_element(self, uid, element_type):
        '''
        Returns the graph element for a given identifier and element 
        type.
        
        Args:
            uid: The unique identifier of the graph element (int).
            element_type: Identifier of the requested graph element (str).
        
        Return:
            A plain object from HyperDex, representing the graph element (dict).
        '''
        return self.hyperdex_client.get('{}_{}'.format(self._graph_name, element_type), uid)
    
    def put_graph_element(self, uid, element_type, element):
        '''
        Store an graph element in HyperDex.
        
        Args:
            uid: The unique identifier of the graph element (int).
            element_type: Identifier of the requested graph element (str).
            
        '''
        self.hyperdex_client.put('{}_{}'.format(self._graph_name,element_type), uid, element)
        
    def add_edge(self, src_vertex, src_type, tar_vertices, edge_type, attributes):
        '''
        Creates an edge between two vertices, which means updating the incoming/outgoing
        edge data structure. 
        
        Args:
            src_vertex: The unique identifier of the source vertex (int).
            src_type: The type of the source vertex (str).
            tar_vertices: List of all target vertices for the requested edge (dict<int, str>). 
            edge_type: The type of the newly created edge (str).
            attributes: The attributes to be attached to this edge (dict). 
        
        Return:
            The unique id of the newly created edge element (int).
        '''
        if self.sysadmin.is_edge_type(edge_type):
            uid = self.sysadmin.get_next_id()
            #print(attributes)
            #store the edge object in the related hyperspace 
            self.hyperdex_client.put('{}_{}'.format(self._graph_name,edge_type), uid, attributes)
        else:
            logging.debug('Edge type not found: {}_{}'.format(self._graph_name, edge_type))
            raise TypeNotFoundException('Edge type not found: {}_{}'.format(self._graph_name, edge_type))
        #add the newly created edge to the index structure
        self.hyperdex_client.map_add('{}_{}'.format(self._graph_name,src_type), src_vertex, {'outgoing_edges' : {uid : edge_type}})
        #iterate over all target vertices, add this edge to the incoming edge structure
        for tar in tar_vertices.keys():
            self.hyperdex_client.map_add('{}_{}'.format(self._graph_name,tar_vertices[tar]), tar, {'incoming_edges' : {uid : edge_type}})
        return uid
    
    def edge_add_target(self, tar_vertex, tar_type, edge_uid, edge_type):
        '''
        Adds a single target to an existing edge, so that it's pointing to multiple target vertices.
        
        Args:
            tar_vertex: The unique identifier of the target vertex (int).
            tar_type: The type of the target vertex (str).
            edge_uid: The unique identifier of the edge (int). 
            edge_type: The type of the edge (str).
        '''
        self.hyperdex_client.map_add('{}_{}'.format(self._graph_name,edge_type), edge_uid, {'target' : tar_vertex})
        #update the incoming edge data structure of the target vertex
        self.hyperdex_client.map_add('{}_{}'.format(self._graph_name,tar_type), tar_vertex, {'incoming_edges' : edge_uid })
    
    def edge_rm_target(self, tar_vertex, tar_type, edge_uid, edge_type):
        '''
        Removes a target vertex from an existing edge object.
        
        Args:
            tar_vertex: The unique identifier of the target vertex (int).
            tar_type: The type of the target vertex (str).
            edge_uid: The unique identifier of the edge (int). 
            edge_type: The type of the edge (str).
        '''
        #TODO check whether this is the last target vertex
        #if this is the case, remove edge
        self.hyperdex_client.map_remove('{}_{}'.format(self._graph_name,edge_type), edge_uid, {'target' : tar_vertex})
        self.hyperdex_client.map_remove('{}_{}'.format(self._graph_name,tar_type), tar_vertex, {'incoming_edges' : edge_uid})
        
    def rm_edge(self, src_vertex, src_type, edge_uid, edge_type):
        '''
        Removes an edge between two or more vertices.
        
        Args:
            src_vertex: The unique identifier of the source vertex (int).
            src_type: The type of the source vertex (str).
            edge_uid: The unique identifier of the edge (int). 
            edge_type: The type of the edge (str).
        '''
        #TODO enable edge removal using only id and type
        self.hyperdex_client.map_remove('{}_{}'.format(self._graph_name,src_type), src_vertex, {'outgoing_edges' : edge_uid })
        edge = self.get_edge_by_id(edge_uid, edge_type)
        for tar in edge['target'].keys():
            self.hyperdex_client.map_remove('{}_{}'.format(self._graph_name,edge['target'][tar]), tar, {'incoming_edges' : edge_uid})
        self.sysadmin.add_obsolete_id(edge_uid)
        self.hyperdex_client.delete('{}_{}'.format(self._graph_name,edge_type), edge_uid)
        
    def get_vertex(self, uid, vertex_type=None):
        '''
        Retrieves the plain vertex object from HyperDex.
        
        Args:
            uid: The unique id of the requested vertex (int).
            vertex_type: The type of the requested vertex (str)
        
        Return:
            A plain dictionary, representing the vertex object (dict).
        '''
        result = None
        if vertex_type is None:
            #if this is vertex has no specified type
            result = self.hyperdex_client.get(self._generic_vertex, uid)
        else:
            if self.sysadmin.is_vertex_type(vertex_type):
                result = self.hyperdex_client.get(self._graph_name + vertex_type, uid)
            else:
                logging.debug('Vertex type not found: ' + self._graph_name + vertex_type)
                raise TypeNotFoundException('Vertex type not found: ' + self._graph_name + vertex_type)
        #if result is still none, the specified vertex does not exist
        if result is None:
            logging.debug('Vertex not found : ' + str(uid))
            raise ElementNotFoundException('Vertex not found : ' + str(uid))
        return result 
    
    def get_edge_by_id(self, uid, edge_type):
        '''
        Returns an plain edge object from HyperDex.
        
        Args:
            uid: The unique id of the requested edge (int).   
            edge_type: The type of the requetsed edge (str).
        
        Return:
            The plain dictionary, representing the edge object (dict).
        '''
        result = None
        if edge_type is None:
            raise TypeError('Illigal argument exception - must not be None')
        else:
            if self.sysadmin.is_edge_type(edge_type):
                result = self.hyperdex_client.get('{}_{}'.format(self._graph_name, edge_type), uid)
            else:
                logging.debug('EdgeType not found: {}_{}'.format(self._graph_name, edge_type))
                raise TypeNotFoundException('EdgeType not found: {}_{}'.format(self._graph_name, edge_type))
        return result 
    
    def search(self, element_type, query):
        #TODO improve search feature
        #TODO add commentary
        return self.hyperdex_client.search('{}_{}'.format(self._graph_name,element_type), query)
    
    def get_edge_by_source(self, source_vertex, edge_type):
        '''
        Returns an list of edge objects, based on the source vertex id.
        
        Args:
            source_vertex: The unique identifier of the source vertex of the requested 
                           edge object (int).
            edge_type: The type of the requested edge (str).
        
        Return:
            List of plain dictionaries, representing the requested edge objects (list<dict>).
        '''
        return self.hyperdex_client.search(self._graph_name + edge_type, {'source' : source_vertex})

    
    def vertex_type_exists(self, vertex_type):
        '''
        Checks whether a vertex type exists or not.
        
        Args:
            vertex_type: The identifier of the vertex type (str).
        
        Return:
            True if the vertex type exists, otherwise False.
        '''
        return self.sysadmin.is_vertex_type(vertex_type)
    
    def edge_type_exists(self, edge_type):
        '''
        Checks whether am edge type exists or not.
        
        Args:
            vertex_type: The identifier of the edge type (str).
        
        Return:
            True if the edge type exists, otherwise False.
        '''
        return self.sysadmin.is_edge_type(edge_type)
        
    def count_elements(self, element_type):
        '''
        Returns the number of graph elements associated with the given
        element type.
        
        Args:
            element_type: The identifier of the graph element (str).
            
        Return:
            Count of graph elements (int).
        '''
        return self.hyperdex_client.count('{}_{}'.format(self._graph_name,element_type), {})

        
