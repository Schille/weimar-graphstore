
import Pyro4, config

class WorkerProcess(object):
    
    def __init__(self,name):
        self.name = name
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        self.proxy_uri = self.ns.lookup('weimar.worker.{}'.format(name))
        self.pyro = None

    
    def say_hello(self):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.say_hello()


    def get_vertex_type(self, graph_name, vertex_type):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_vertex_type(graph_name, vertex_type)
        
    def get_edge_type(self, graph_name, edge_type):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_edge_type(graph_name, edge_type)
    
    def create_vertex_type(self, graph_name, type_name, vertex_type_def):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.create_vertex_type(graph_name, type_name, vertex_type_def)
        
            
    def create_edge_type(self, graph_name, type_name, edge_type_def):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.create_edge_type(graph_name, type_name, edge_type_def)

    def insert_vertex(self, graph_name, vertex_type, attributes):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.insert_vertex(graph_name, vertex_type, attributes)
    
    def get_vertex(self, graph_name, uid, vertex_type):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_vertex(graph_name, uid, vertex_type)
    
    def get_type_definition(self, graph_name, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_type_definition(graph_name, type_name)

    def count(self, graph_name, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.count(graph_name, type_name)
    
    def get_elements(self, graph_name, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_elements(graph_name, type_name)

    def remove_type(self, graph_name, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.remove_type(graph_name, type_name)
    
    def get_property(self, graph_name, uid, type_name, key):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_property(graph_name, uid, type_name, key)      
    
    def set_property(self, graph_name, uid, type_name, key, value):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.set_property(graph_name, uid, type_name, key, value) 
    
    def get_property_keys(self, graph_name, uid, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.get_property_keys(graph_name, uid, type_name)
    
    def v_add_edge(self, graph_name, uid, type_name, target_type, target_uid,\
                  edge_type, struct_attr, unstruc_attr):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.v_add_edge(graph_name, uid, type_name, target_type, target_uid,\
                  edge_type, struct_attr, unstruc_attr)
    
    def v_rm_edge(self, graph_name, uid, type_name, edge_uid, edge_type):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.v_rm_edge(graph_name, uid, type_name, edge_uid, edge_type)
    
    def v_get_outgoing_edges(self, graph_name, uid, type_name, edge_type):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.v_get_outgoing_edges(graph_name, uid, type_name, edge_type)
    
    def v_get_incoming_edges(self, graph_name, uid, type_name, edge_type):
        #return edges
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.v_get_incoming_edges(graph_name, uid, type_name, edge_type)
    
    def e_get_target(self, graph_name, uid, type_name):
        #return the edge object's target vertices
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.e_get_target(graph_name, uid, type_name)
    
    def e_get_source(self, graph_name, uid, type_name):
        #return the edge object's source vertex
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.e_get_source(graph_name, uid, type_name)
    
    def e_add_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        #add a target vertex to edge
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.e_add_target(graph_name, uid, type_name, vertex_uid, vertex_type)
    
    def e_rm_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        #remove target vertex from edge
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.e_rm_target(graph_name, uid, type_name, vertex_uid, vertex_type)
        
    def remove(self, graph_name, uid, type_name):
        if(self.pyro is None):
            self.pyro = Pyro4.Proxy(self.proxy_uri)
        return self.pyro.remove(graph_name, uid, type_name)


        