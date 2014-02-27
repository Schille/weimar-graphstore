"""
.. module:: sysadmin.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from hyperdex.client import HyperClientException
import logging
import os

SYSTEM = 'system'
NEXT_ID = 'nextid'
OBSOLETE_ID = 'obsolete_id'
VERTEX_TYPES = 'vertex_types'
EDGE_TYPES = 'edge_types'

class SysAdmin():
    '''
    The SysAdmin class deals with the system's internal data, keeping track of the
    created classes of vertices and edges. It manages unique identifiers and 
    obsolete ids.  
    '''


    def __init__(self, address, port, hyperdex_client, hyperdex_admin):
        '''
        The SysAdmin constructor initializes the HyperDex coordinator process it's 
        working on.
        
        Remark: The address and port argument is necessary, due to a workaround
        for HyperDex when creating new spaces and the operation is performed on slow systems.
        
        Args:
            address: A valid IP address of the HyperDex coordinator process (str).
            port: The port number of the HyperDex coordinator process (int).
            hyperdex_client: A hyperdex.client.Client instance (Client).
            hyperdex_admin: A hyperdex.admin.Admin (Admin).
        '''
        self.hyperdex_client = hyperdex_client
        self._address = address
        self._port = port
        self.hyperdex_admin = hyperdex_admin
        
    def validate_database(self):
        '''
        Validates whether the database contains already the required hyperspace,
        needed by the SysAdmin.
        '''
        try:
            self.hyperdex_client.get(SYSTEM, VERTEX_TYPES)
            logging.info('system space availabe')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                space = '''
                space ''' + SYSTEM + '''
                key symbol
                attributes list(string) value
                '''
                logging.info('Creating initial system space')
                self.hyperdex_admin.add_space(space)
                return 
            logging.error(e)
    
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
            self.hyperdex_client.list_rpush(SYSTEM, element_types, {'value' : type_name})
        except Exception, e:
            #if the data structure is not yet ready, the following code tries to 
            #insert it
            logging.info('Created first ' + element_types)
            #Debris
            for i in xrange(5):#5 attempts to insert the data
                try:
                    self.hyperdex_client.put(SYSTEM, element_types, {'value' : []})
                    break
                except Exception, e:
                    logging.warn('need a further attempt ' + element_types)
                    # HyperDex seems to need a particular time to create the new space
                    os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                    if i == 4:
                        raise e
            #TODO refactor this code
            self.hyperdex_client.list_rpush(SYSTEM, element_types, {'value' : type_name})
    
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
        #first, get all known element types
        result = self.hyperdex_client.get(SYSTEM, element_type)['value']
        if result is not None:
            #if the requested type exists it can also be removed
            result.remove(element_name)
        #second, set the resulting set of known element types
        self.hyperdex_client.put(SYSTEM, element_type, { 'value' : result})
    
    def get_next_id(self):
        '''
        Returns a new id, which can be assigned to a newly created graph element.
        This is synchronized by the database in case multiple workers a running.
        
        Returns:
            A unique and free id which can be assigned to a newly created graph
            element.
        
        '''
        #TODO This code needs to be refactored to use incr() of HyperDex
        #try whether there is already a next id
        uid = self.hyperdex_client.get(SYSTEM, NEXT_ID)
        if uid is None:#this case occurs when no graph element ever has been created
            self.hyperdex_client.put(SYSTEM, NEXT_ID, { 'value' : [str(-100000)]} )
            return -100001#the first id ever returned
        else:
            uid = int(uid['value'][0])
            #TODO this is a race condition, when working with multiple workers
            self.hyperdex_client.put(SYSTEM, NEXT_ID, { 'value' : [str(uid + 1)]})
            return uid# return the resulting id
    
    def is_vertex_type(self, vertex_type):
        '''
        Returns bool, whether the given vertex type identifier is known to the SysAdmin.
        
        Args:
            vertex_type: A vertex type identifier (str).
        
        Returns:
            True if the vertex types exists, otherwise False.
        '''
        types = self.hyperdex_client.get(SYSTEM, VERTEX_TYPES)
        if types is None:
            return False
        else:
            if vertex_type in  types['value']:
                return True
            else:
                return False

    def is_edge_type(self, edge_type):
        '''
        Returns bool, whether the given edge type identifier is known to the SysAdmin.
        
        Args:
            vertex_type: A edge type identifier (str).
        
        Returns:
            True if the edge types exists, otherwise False.
        '''
        types = self.hyperdex_client.get(SYSTEM, EDGE_TYPES)
        if types is None:
            return False
        else:
            if edge_type in types['value']:
                return True
            else:
                return False
        
        
    def add_obsolete_id(self, uid):
        #TODO refactor the following code
        '''
        Adds the given id to the pool of obsolete ids, which could be recycled.
        
        Args:
            uid: The id of the deleted graph element (int).
        '''
        try:
            #try to push the first obsolete id
            self.hyperdex_client.list_rpush(SYSTEM, OBSOLETE_ID, {'value' : str(uid)})
        except Exception:
            logging.info('created first ' + OBSOLETE_ID)
            try:
                self.hyperdex_client.put(SYSTEM, OBSOLETE_ID, {'value' : []})
            except Exception:
                # HyperDex seems to need a particular time to create the new space
                os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                self.hyperdex_client.put(SYSTEM, OBSOLETE_ID, {'value' : []})
            self.hyperdex_client.list_rpush(SYSTEM, OBSOLETE_ID, {'value' : str(uid)})
            
        
            
           
           
           
           
           
           
           
            
               
                
        