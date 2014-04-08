
import Pyro4, config, signal, time
import multiprocessing as mp
from remote.serverprocesses import NameServer

class WeimarGraphServer(mp.Process):
    
    def __init__(self, worker_pool):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(WeimarGraphServer, self).__init__(name='weimar-graphserver')
        self.worker_pool = worker_pool
        self._running = mp.Value('i', 1)
        #start outside name server
        self._o_nssvr = NameServer(config.WEIMAR_ADDRESS_OUTSIDE, config.WEIMAR_PORT_OUTSIDE)
        
        
        #start a new server instance, pass handle on the worker pool
        self.server =  Server(self.worker_pool)
        self.type_svr = RemoteElementType(self.worker_pool)
        self.element_svr = RemoteGraphElement(self.worker_pool)
    
        self.daemon = Pyro4.core.Daemon(host=config.WEIMAR_ADDRESS_OUTSIDE)
        self.server_uri = self.daemon.register(self.server)
        self.type_svr_uri = self.daemon.register(self.type_svr)
        self.element_svr_uri = self.daemon.register(self.element_svr)
    
    
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_OUTSIDE, port=config.WEIMAR_PORT_OUTSIDE)
        self.ns.register('weimar.server.api', self.server_uri)
        self.ns.register('weimar.server.type', self.type_svr_uri)
        self.ns.register('weimar.server.element', self.element_svr_uri)
        
        self.start()
        time.sleep(1)
    
    def run(self):
        print('[Info] Starting: weimar-graphserver')
        self.daemon = Pyro4.core.Daemon(host=config.WEIMAR_ADDRESS_OUTSIDE)
        Pyro4.config.COMMTIMEOUT=3.5
        self.daemon.requestLoop(loopCondition=lambda:self._running.value)
        #shutdown was issued
        self.daemon.close()
    
    def shutdown(self):
        self._running.value = 0
        self._o_nssvr.shutdown()
        time.sleep(2)
        self.terminate()
        

class Server(object):
    '''
    classdocs
    '''


    def __init__(self, worker_pool):
        '''
        Constructor.
        '''
        self._workerpool = []
        self._worker_available = worker_pool
    
    def insert_vertex(self, graph_name, vertex_type, attributes):
        worker = self._worker_available.get(True)
        result = worker.insert_vertex(graph_name, vertex_type, attributes)
        self._worker_available.put(worker)
        return result
        
    def get_vertex_type(self, graph_name, vertex_type):
        worker = self._worker_available.get(True)
        result = worker.get_vertex_type(graph_name, vertex_type)
        self._worker_available.put(worker)
        return result
    
    def get_edge_type(self, graph_name, edge_type):
        worker = self._worker_available.get(True)
        result = worker.get_edge_type(graph_name, edge_type)
        self._worker_available.put(worker)
        return result
    
    def create_vertex_type(self, graph_name, type_name, vertex_type_def):
        worker = self._worker_available.get(True)
        result = worker.create_vertex_type(graph_name, type_name, vertex_type_def)
        self._worker_available.put(worker)
        return result
    
    def create_edge_type(self, graph_name, type_name, edge_type_def):
        worker = self._worker_available.get(True)
        result = worker.create_edge_type(graph_name, type_name, edge_type_def)
        self._worker_available.put(worker)
        return result
    
    def get_vertex(self, graph_name, uid, vertex_type):
        worker = self._worker_available.get(True)
        result = worker.get_vertex(graph_name, uid, vertex_type)
        self._worker_available.put(worker)
        return result

    def say_hello(self):
        worker = self._worker_available.get(True)
        result = worker.say_hello()
        self._worker_available.put(worker)
        return result


class RemoteElementType(object):
    
    def __init__(self, worker_pool):
        '''
        Constructor.
        '''
        self._workerpool = []
        self._worker_available = worker_pool
    
    def get_type_definition(self, graph_name, type_name):
        worker = self._worker_available.get(True)
        result = worker.get_type_definition(graph_name, type_name)
        self._worker_available.put(worker)
        return result

    def count(self, graph_name, type_name):
        worker = self._worker_available.get(True)
        result = worker.count(graph_name, type_name)
        self._worker_available.put(worker)
        return result
    
    def get_elements(self, graph_name, type_name):
        worker = self._worker_available.get(True)
        result = worker.get_elements(graph_name, type_name)
        self._worker_available.put(worker)
        return result

    def remove(self, graph_name, type_name):
        worker = self._worker_available.get(True)
        result = worker.remove_type(graph_name, type_name)
        self._worker_available.put(worker)
        return result

class RemoteGraphElement(object):
    
    def __init__(self, worker_pool):
        '''
        Constructor.
        '''
        self._workerpool = []
        self._worker_available = worker_pool
    
    def get_property(self, graph_name, uid, type_name, key):
        worker = self._worker_available.get(True)
        result = worker.get_property(graph_name, uid, type_name, key)
        self._worker_available.put(worker)
        return result
    
    def set_property(self, graph_name, uid, type_name, key, value):
        worker = self._worker_available.get(True)
        result = worker.set_property(graph_name, uid, type_name, key, value)
        self._worker_available.put(worker)
        return result
    
    def get_property_keys(self, graph_name, uid, type_name):
        worker = self._worker_available.get(True)
        result = worker.get_property_keys(graph_name, uid, type_name)
        self._worker_available.put(worker)
        return result
    
    def v_add_edge(self, graph_name, uid, type_name, target_type, target_uid,\
                  edge_type, struct_attr, unstruc_attr):
        worker = self._worker_available.get(True)
        result = worker.v_add_edge(graph_name, uid, type_name, target_type, target_uid,\
                  edge_type, struct_attr, unstruc_attr)
        self._worker_available.put(worker)
        return result
    
    def v_rm_edge(self, graph_name, uid, type_name, edge_id, edge_type):
        worker = self._worker_available.get(True)
        result = worker.v_rm_edge(graph_name, uid, type_name, edge_id, edge_type)
        self._worker_available.put(worker)
        return result
    
    def v_get_outgoing_edges(self, graph_name, uid, type_name, edge_type):
        worker = self._worker_available.get(True)
        result = worker.v_get_outgoing_edges(graph_name, uid, type_name, edge_type)
        self._worker_available.put(worker)
        return result
    
    def v_get_incoming_edges(self, graph_name, uid, type_name, edge_type):
        worker = self._worker_available.get(True)
        result = worker.v_get_incoming_edges(graph_name, uid, type_name, edge_type)
        self._worker_available.put(worker)
        return result
    
    def e_get_target(self, graph_name, uid, type_name):
        worker = self._worker_available.get(True)
        result = worker.e_get_target(graph_name, uid, type_name)
        self._worker_available.put(worker)
        return result
    
    def e_get_source(self, graph_name, uid, type_name):
        worker = self._worker_available.get(True)
        result = worker.e_get_source(graph_name, uid, type_name)
        self._worker_available.put(worker)
        return result
    
    def e_add_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        worker = self._worker_available.get(True)
        result = worker.e_add_target(graph_name, uid, type_name, vertex_uid, vertex_type)
        self._worker_available.put(worker)
        return result
    
    def e_rm_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        worker = self._worker_available.get(True)
        result = worker.e_rm_target(graph_name, uid, type_name, vertex_uid, vertex_type)
        self._worker_available.put(worker)
        return result
        
    def remove(self, graph_name, uid, type_name):
        worker = self._worker_available.get(True)
        result = worker.remove(graph_name, uid, type_name)
        self._worker_available.put(worker)
        return result
    
    
        
