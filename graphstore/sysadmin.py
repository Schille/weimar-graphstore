"""
.. module:: sysadmin.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from hyperdex.client import HyperClientException
import logging
import os


ID = 'id'
VERTEX_TYPES = 'vertex_types'
EDGE_TYPES = 'edge_types'

class SysAdmin():
    '''
    The SysAdmin class deals with the system's internal data, keeping track of the
    created classes of vertices and edges. It manages unique identifiers and 
    obsolete ids.  
    '''


    def __init__(self, address, port, graph_name,  hyperdex_client, hyperdex_admin):
        '''
        The SysAdmin constructor initializes the HyperDex coordinator process it's 
        working on.
        
        Remark: The address and port argument is necessary, due to a workaround
        for HyperDex when creating new spaces on slow systems.
        
        Args:
            address: A valid IP address of the HyperDex coordinator process (str).
            port: The port number of the HyperDex coordinator process (int).
            graph_name: The identifier of the graph to be opened for this SysAdmin (str). 
            hyperdex_client: A hyperdex.client.Client instance (Client).
            hyperdex_admin: A hyperdex.admin.Admin (Admin).
        '''
        self.hyperdex_client = hyperdex_client
        self._address = address
        self._port = port
        self.hyperdex_admin = hyperdex_admin
        self.graph_id = graph_name
        self._system = graph_name + '_system'
        self._next_id = graph_name + '_next_id'
        self._id_sys = graph_name + '_id_sys'
        self._obsolete_id = graph_name + '_obsolete_id'
        
    def validate_database(self):
        '''
        Validates whether the database contains already the required hyperspace,
        needed by the SysAdmin.
        '''
        try:
            self.hyperdex_client.get(self._system, VERTEX_TYPES)
            logging.info('system space availabe')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                space = '''
                space ''' + self._system + '''
                key symbol
                attributes set(string) value
                '''
                logging.info('Creating initial space ' + self._system)
                self.hyperdex_admin.add_space(space)
            #logging.error(e)
        try:
            self.hyperdex_client.get(self._id_sys, self._obsolete_id )
            logging.info('id sys space availabe')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                space = '''
                space ''' + self._id_sys + '''
                key symbol
                attributes set(int) value
                '''
                logging.info('Creating initial space ' + self._id_sys)
                self.hyperdex_admin.add_space(space)
                try:
                    self.hyperdex_client.put_if_not_exist(self._id_sys, self._obsolete_id , {'value' : set([])})
                except Exception, e:
                    logging.warn('need a further attempt ' + self._obsolete_id )
                    # HyperDex seems to need a particular time to be ready to insert new data
                    os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                    self.hyperdex_client.put_if_not_exist(self._id_sys, self._obsolete_id , {'value' : set([])})
            #logging.error(e)
        try:
            self.hyperdex_client.get(ID, self._next_id)
            logging.info('next id space availabe')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                space = '''
                space ''' + ID + '''
                key symbol
                attributes int value
                '''
                logging.info('Creating initial next id space')
                self.hyperdex_admin.add_space(space)
            #logging.error(e)
            
    
    def add_vertex_type(self, vertex_type):
        '''
        Inserts a vertex type as 'now available' in the SysAdmin
        
        Args:
            vertex_type: The vertex type identifier (str).
        '''
        self.__add_element_type(VERTEX_TYPES, vertex_type)
    
    def add_edge_type(self, edge_type):
        '''
        Inserts an edge type as 'now available' in the SysAdmin
        
        Args:
            edge_type: The edge type identifier (str).
        '''
        self.__add_element_type(EDGE_TYPES, edge_type)
    
    def __add_element_type(self, element_types, type_name):
        '''
        Inserts an element as 'now available' in the SysAdmin
        
        Args:
            edge_type: The element type identifier (str).
        '''
        try:
            #push the new element to the data structure
            self.hyperdex_client.set_add(self._system, element_types, {'value' : type_name})
        except Exception, e:
            #if the data structure is not yet ready, the following code tries to 
            #insert it
            logging.info('Created first ' + element_types)
            #Debris
            for i in xrange(5):#5 attempts to insert the data
                try:
                    self.hyperdex_client.put(self._system, element_types, {'value' : set([])})
                    break
                except Exception, e:
                    logging.warn('need a further attempt ' + element_types)
                    # HyperDex seems to need a particular time to create the new space
                    os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                    if i == 4:
                        raise e
            #TODO refactor this code
            self.hyperdex_client.set_add(self._system, element_types, {'value' : type_name})
    
    def rm_vertex_type(self, vertex_type):
        '''
        Removes the vertex type from the SysAdmin
        
        Args:
            vertex_type: The vertex type identifier (str).
        '''
        self.__rm_element_type(VERTEX_TYPES, vertex_type)
        
    def rm_edge_type(self, edge_type):
        '''
        Removes the edge type from the SysAdmin
        
        Args:
            edge_type: The edge type identifier (str).
        '''
        self.__rm_element_type(EDGE_TYPES, edge_type)
    
    
    def __rm_element_type(self, element_type, element_name):
        self.hyperdex_client.set_remove(self._system, element_type, { 'value' : element_name})
    
    def get_next_id(self):
        '''
        Returns a new id, which can be assigned to a newly created graph element.
        This is synchronized by the database in case multiple workers a running.
        
        Returns:
            A unique and free id which can be assigned to a newly created graph
            element.
        
        '''
        #try whether there is already a next id
        uid = self.hyperdex_client.get(ID, self.graph_id)
        if uid is None:#this case occurs when no graph element ever has been created
            if self.hyperdex_client.put_if_not_exist(ID, self.graph_id, { 'value' : -100000} ):
                return -100001#the first id ever returned
            else:
                return self.get_next_id()
        else:
            self.hyperdex_client.atomic_add(ID, self.graph_id, { 'value' : 1})
            return uid['value']# return the resulting id
    
    def is_vertex_type(self, vertex_type):
        '''
        Returns bool, whether the given vertex type identifier is known to the SysAdmin.
        
        Args:
            vertex_type: A vertex type identifier (str).
        
        Returns:
            True if the vertex types exists, otherwise False.
        '''
        types = self.hyperdex_client.get(self._system, VERTEX_TYPES)
        if types is None:
            return False
        else:
            return vertex_type in types['value']

    def is_edge_type(self, edge_type):
        '''
        Returns bool, whether the given edge type identifier is known to the SysAdmin.
        
        Args:
            vertex_type: A edge type identifier (str).
        
        Returns:
            True if the edge types exists, otherwise False.
        '''
        types = self.hyperdex_client.get(self._system, EDGE_TYPES)
        if types is None:
            return False
        else:
            return edge_type in types['value']
        
        
    def add_obsolete_id(self, uid):
        #TODO refactor the following code
        '''
        Adds the given id to the pool of obsolete ids, which could be recycled.
        
        Args:
            uid: The id of the deleted graph element (int).
        '''
        try:
            #try to push the first obsolete id
            self.hyperdex_client.set_add(self._id_sys, self._obsolete_id , {'value' : uid})
            return
        except Exception:
            logging.info('created first ' + self._obsolete_id )
            try:
                if self.hyperdex_client.put_if_not_exist(self._id_sys, self._obsolete_id , {'value' : set([uid])}):
                    return
            except Exception:
                # HyperDex seems to need a particular time to create the new space
                os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                if self.hyperdex_client.put_if_not_exist(self._id_sys, self._obsolete_id , {'value' : set([uid])}):
                    return
        logging.error('could not add to obsolte id: ' + uid)
            
    def register_element_id(self, element_type, uid):
        if self.is_vertex_type(element_type) or self.is_edge_type(element_type):
            try:
                #try to push the first obsolete id
                self.hyperdex_client.set_add(self._id_sys, element_type, {'value' : uid})
            except Exception:
                logging.info('register first id for: ' + element_type + ' in ' + self._id_sys)
                try:
                    self.hyperdex_client.put(self._id_sys, element_type, {'value' : set([uid])})
                except Exception:
                    # HyperDex seems to need a particular time to create the new space
                    os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                    self.hyperdex_client.put(self._id_sys, element_type, {'value' : set([uid])})
    
    def optout_element_id(self, element_type, uid):
        if self.is_vertex_type(element_type) or self.is_edge_type(element_type):
            self.hyperdex_client.set_remove(self._id_sys, element_type)
            
            
    
           
           
           
           
           
           
            
               
                
        