"""
.. module:: typeadmin.py
   :platform: Linux
   :synopsis: The HyperDexGraph API

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
from hyperdex.client import HyperClientException
import os
import logging


class TypeAdmin():
    '''
    The TypeAdmin is responsible for the management of the graph element
    types used in HyperDexGraph. It tracks the creation of hyperspaces and 
    validates whether a certain type exists or not.
    '''


    def __init__(self, address, port, graph_name, hyperdex_client, hyperdex_admin):
        '''
        Constructor.
        '''
        self.hyperdex_admin = hyperdex_admin
        self._address = address
        self._port = port
        self.hyperdex_client = hyperdex_client
        self._graph_name = graph_name
        self._type_description = graph_name + '_space_description'
    
    def validate_database(self):
        try:
            self.hyperdex_client.get(self._type_description, 1)
            logging.info('Space_description available')
        except HyperClientException, e:
            if e.symbol() == 'HYPERDEX_CLIENT_UNKNOWNSPACE':
                space = '''
                space ''' + self._type_description  + ''' 
                key spacename
                attributes map(string,string) attr
                '''
                logging.info('Creating initial space_description')
                self.hyperdex_admin.add_space(space)
                return 
            logging.error(str(e))
    
        
    def create_element_type(self, element_name, attributes):
        '''
        Creates a new element type, which means the creation of a new hyperspace
        in HyperDex. Each graph element type relates to its own hyperspace, in which all
        entities of that type are stored.
        
        Args:
            element_name: The  identifier of the requested element type (str).
            attributes: The type's definition attributes (list<tuples<str, str>>)
        '''
        #currently all attributes are assigned to the same subspace - which is 
        #highly inefficient
        #TODO find a appropriate solution to partition subspaces for newly created types
        attr = self._dictify_attributes(attributes) #convert the attribute definition to a dictionary
        space = 'space ' + '{}_{}'.format(self._graph_name,element_name) + '\nkey int id\nattributes\n' \
            + self._stringify_attributes(attributes) + ',\n string value'#convert the dictionary to a space declaration 
        #create a new hyperspace for the requested graph element type
        self.hyperdex_admin.add_space(space)
        #TODO refactor this debris
        for i in xrange(5):
            try:
                self.hyperdex_client.put(self._type_description, element_name, {'attr' : attr})
                break
            except Exception, e:
                logging.warn('Need a further attempt ' + self._type_description)
                os.system('hyperdex wait-until-stable -h {} -p {}'.format(self._address, self._port))
                if i == 4:
                        raise e
    
    def remove_element_type(self, element_name):
        '''
        Removes permanently the hyperspace for the given element name. This
        includes also all entities within this space.
        
        Args:
            element_name: The identifier of this element type (str).
        '''
        self.hyperdex_admin.rm_space('{}_{}'.format(self._graph_name,element_name))
        self.hyperdex_client.delete(self._type_description, element_name)
    
    def get_type_description(self, space_name):
        '''
        Returns the description of the given element type. This is the
        hyperspace definition.
        
        Args:
            space_name: The identifier of the element type (str).
        
        Return:
            List of declared attributes for the given element type (list<tuple<str, str>>). 
        '''
        #get the type description
        description = self.hyperdex_client.get(self._type_description, space_name)
        if description is not None:
            result = []
            #rebuild the space description 
            for key, value in description['attr'].iteritems():
                result.append((value, key))
            return result
        return None
            
    def _stringify_attributes(self, attributes):
        '''
        Formats an dictionary of attributes as string, readable for HyperDex.
        
        Args:
            attributes: The attributes to be written as string (dict).
        
        Return:
            The give dictionary of attributes as string (str).
        '''
        result = ''
        for attr in attributes:
            result = '{} {} {},\n'.format(result, attr[0], attr[1])
        return result[:-2]
    
    def _dictify_attributes(self, attributes):
        '''
        Converts a list of user defined attributes of the element type definition
        to a dictionary.
        
        Args:
            attributes: List of tuples to  be converted (list<tuble<str,str>>)
        '''
        result = {}
        for attr in attributes:
            result[attr[1]] = attr[0]
        return result
            
        