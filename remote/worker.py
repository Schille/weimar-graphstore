'''
Created on Mar 17, 2014

@author: mschilonka
'''

import Pyro4
import config
import random
import signal, sys, time
import multiprocessing as mp
from graph.graphelement import Vertex, Edge 
from graph.elementtype import EdgeType, VertexType
from graph.requestgraphelements import RequestEdgeType, RequestVertexType, RequestVertex
from graph.hyperdexgraph import HyperDexGraph
import traceback

def start_worker(num_threads):

    worker_p = []
    
    for i in xrange(0, num_threads):
        worker_p.append(WorkerProcess())
        time.sleep(1)
        
    
    def signal_handler(signum, frame):
        print('\nShutting down Worker...')
        [p.shutdown() for p in worker_p]
        [p.join() for p in worker_p]
        print('\nShutting down Worker...Done')
        sys.exit()
    
    signal.signal(signal.SIGINT, signal_handler)
    
    ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
    registry_uri = ns.lookup('weimar.worker.registry')
    registry = Pyro4.Proxy(registry_uri)
    
    print('Started {} work_p processes'.format(len(worker_p)))
    Pyro4.config.COMMTIMEOUT=3.5
    while(True):
        for process in worker_p:
            if not process.is_alive():
                print('Worker process {} stopped working unexpectedly.'.format(process.name))
                try:
                    registry.unregister(process.name)
                except Pyro4.errors.ConnectionClosedError:
                    print('[Warn] Weimar server is no longer available.')
                except Exception, ex:
                    print(ex)
                worker_p.remove(process)         
        time.sleep(2)
        if(len(worker_p) == 0):
            break
        
    
    

class WorkerProcess(mp.Process):
    
    def __init__(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        #get relevant information from the weimar sever
        registry_uri = self.ns.lookup('weimar.worker.registry')
        self.registry = Pyro4.Proxy(registry_uri)
        self.workername = self.registry.register()
        super(WorkerProcess, self).__init__(name=self.workername)
        self._running = mp.Value('i', 1)
        self.start()
    
    def run(self):
        Pyro4.config.COMMTIMEOUT=5.5
        #create process
        my_ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)
        self.daemon = Pyro4.core.Daemon(my_ip)
        hyperdex_uri = self.ns.lookup('hyperdex.properties')
        
        hyperdex = Pyro4.Proxy(hyperdex_uri)
        
        hyperdex_ip = hyperdex.get_ip()
        hyperdex_port = hyperdex.get_port()
        
        #create worker object for Pyro
        self.worker = Worker(self.workername,hyperdex_ip, hyperdex_port, self)
        worker_uri = self.daemon.register(self.worker)
        #register object at the weimar server
        print(self.workername)
        self.ns.register('weimar.worker.{}'.format(self.workername), worker_uri)
        
        self.daemon.requestLoop(loopCondition=lambda:self._running.value)
        #shutdown issued
        self.daemon.close()
        self.registry.unregister(self.workername)

        
    def shutdown(self):
        self._running.value = 0
        print('[Info] Shutting down: ' + self.name)
        


class Worker(object):
    '''
    classdocs
    '''


    def __init__(self, worker_name, hyperdex_ip, hyperdex_port, process):
        '''
        Constructor
        '''
        self.name = worker_name
        self.graphs = {}
        self.hyperdex_ip = hyperdex_ip
        self.hyperdex_port = hyperdex_port
        self._process = process
    
    def say_hello(self):
        return 'Hello from {}'.format(self.name)
    
    def shutdown(self, code):
        print('[Warn] Worker {} is requested to shut down. Server code: {}'.format(self.name, code))
        #todo handle server code
        if(code == 1000):
            self._process.shutdown()
        

    def get_vertex_type(self, graph_name, vertex_type):
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            try:
                return self.graphs[graph_name].get_vertex_type(vertex_type)
                return True
            except:
                return False
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            try:
                return self.graphs[graph_name].get_vertex_type(vertex_type)
                return True
            except:
                return False
        
    def get_edge_type(self,graph_name , edge_type):
        if(self.graphs.has_key(graph_name)):
            try:
                return self.graphs[graph_name].get_edge_type(edge_type)
                return True
            except:
                return False
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            try:
                return self.graphs[graph_name].get_edge_type(edge_type)
                return True
            except:
                return False
    
    def create_vertex_type(self, graph_name, type_name, vertex_type_def):
        print('Create vertex type')
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            try:
                self.graphs[graph_name].create_vertex_type(RequestVertexType(type_name, *vertex_type_def))
                return True
            except:
                traceback.print_exc()
                return False  
        else:
            print('Create new graph {} on HyperDex at {}:{}'.format(graph_name, self.hyperdex_ip, self.hyperdex_port))
            print(type_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            try:
                self.graphs[graph_name].create_vertex_type(RequestVertexType(type_name, *vertex_type_def))
                return True
            except Exception, e:
                traceback.print_exc()
                print(e)
                return False 
        
            
    def create_edge_type(self, graph_name, type_name, edge_type_def):
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            try:
                self.graphs[graph_name].create_edge_type(RequestEdgeType(type_name, *edge_type_def))
                return True
            except:
                return False
        else:
            print('Create new graph {} on HyperDex at {}:{}'.format(graph_name, self.hyperdex_ip, self.hyperdex_port))
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            try:
                self.graphs[graph_name].create_edge_type(RequestEdgeType(type_name, *edge_type_def))
                return True
            except:
                return False

    def insert_vertex(self, graph_name, vertex_type, attributes):
        if(self.graphs.has_key(graph_name)):
            print('Insert Vertex')
            return self.graphs[graph_name].insert_vertex(\
                RequestVertex(VertexType(self.graphs[graph_name]._storage , vertex_type.split(':', 1)[1]),attributes))._uid
        else:
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            return self.graphs[graph_name].insert_vertex(\
                RequestVertex(VertexType(self.graphs[graph_name]._storage , vertex_type.split(':', 1)[1]),attributes))._uid
            
            
    def get_vertex(self, graph_name, uid, vertex_type):
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            try:
                self.graphs[graph_name].get_vertex(uid, vertex_type)
                return True
            except:
                return False
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            try:
                self.graphs[graph_name].get_vertex(uid, vertex_type)
                return True
            except:
                return False
    
    def get_type_definition(self, graph_name, type_name):
        print(type_name)
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            if(t[0] == 'vertex'):
                return self.graphs[graph_name].get_vertex_type(t[1]).get_type_definition()
            elif(t[0] == 'edge'):
                return self.graphs[graph_name].get_edge_type(t[1]).get_type_definition()
            else:
                #TODO
                raise Exception()
        else:
            #TODO may the graph has been delete 
            raise Exception()

    def count(self, graph_name, type_name):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            if(t[0] == 'vertex'):
                return self.graphs[graph_name].get_vertex_type(t[1]).count()
            elif(t[0] == 'edge'):
                return self.graphs[graph_name].get_edge_type(t[1]).count()
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                return self.graphs[graph_name].get_vertex_type(t[1]).count()
            elif(t[0] == 'edge'):
                return self.graphs[graph_name].get_edge_type(t[1]).count()
            else:
                #TODO
                raise Exception()
    
    def get_elements(self, graph_name, type_name):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            if(t[0] == 'vertex'):
                return self.graphs[graph_name].get_vertex_type(t[1]).get_vertices()
            elif(t[0] == 'edge'):
                return self.graphs[graph_name].get_edge_type(t[1]).get_edges()
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                return self.graphs[graph_name].get_vertex_type(t[1]).get_vertices()
            elif(t[0] == 'edge'):
                return self.graphs[graph_name].get_edge_type(t[1]).get_edges()
            else:
                #TODO
                raise Exception()

    def remove_type(self, graph_name, type_name):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            print('Removing: ' + type_name)
            if(t[0] == 'vertex'):
                self.graphs[graph_name].get_vertex_type(t[1]).remove()
            elif(t[0] == 'edge'):
                self.graphs[graph_name].get_edge_type(t[1]).remove()
            else:
                #TODO
                raise Exception('Something went wrong.')
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                self.graphs[graph_name].get_vertex_type(t[1]).remove()
            elif(t[0] == 'edge'):
                self.graphs[graph_name].get_edge_type(t[1]).remove()
            else:
                #TODO
                raise Exception()
    
    def get_property(self, graph_name, uid, type_name, key):
        print('get property')
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).get_property(key)
            elif(t[0] == 'edge'):
                result = Edge(uid, t[1], self.graphs[graph_name]._storage).get_property(key)
                print(result)
                return result
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).get_property(key)
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).get_property(key)
            else:
                #TODO
                raise Exception()       
    
    def set_property(self, graph_name, uid, type_name, key, value):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            #TODO this is a workaround
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).set_property(key, value)
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).set_property(key, value)
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).set_property(key, value)
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).set_property(key, value)
            else:
                #TODO
                raise Exception() 
    
    def get_property_keys(self, graph_name, uid, type_name):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            #TODO this is a workaround
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).get_property_keys()
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).get_property_keys()
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).get_property_keys()
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).get_property_keys()
            else:
                #TODO
                raise Exception()
    
    def v_add_edge(self, graph_name, uid, type_name, target_type, target_uid,\
                  edge_type, struct_attr, unstruc_attr):
        print('add edge from {}:{} to {}:{}'.format(uid, type_name, target_uid, target_type))
        print(struct_attr)
        if(self.graphs.has_key(graph_name)):
            return Vertex(uid, type_name, self.graphs[graph_name]._storage)\
            .add_edge(Vertex(target_uid, target_type, self.graphs[graph_name]._storage),\
                       edge_type, struct_attr, unstruc_attr)._uid
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            return Vertex(uid, type_name, self.graphs[graph_name]._storage)\
            .add_edge(Vertex(target_uid, target_type, self.graphs[graph_name]._storage),\
                       edge_type, struct_attr, unstruc_attr)._uid
    
    def v_rm_edge(self, graph_name, uid, type_name, edge_uid, edge_type):
        #remove edge
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            Vertex(uid, type_name, self.graphs[graph_name]._storage)\
            .rm_edge(Edge(edge_uid, edge_type, self.graphs[graph_name]._storage))
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            Vertex(uid, type_name, self.graphs[graph_name]._storage)\
            .rm_edge(Edge(edge_uid, edge_type, self.graphs[graph_name]._storage))
    
    def v_get_outgoing_edges(self, graph_name, uid, type_name, edge_type):
        print('outgoing edges')
        print(type_name)
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            if(edge_type is None):
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                    .get_outgoing_edges()
                return {x._uid : x._element_type for x in result}    
            else:
                print('Debug')
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                .get_outgoing_edges(EdgeType(self.graphs[graph_name]._storage, edge_type))
                print(result)
                return {x._uid : x._element_type for x in result}
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            self.graphs[graph_name] = None
            if(edge_type is None):
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                    .get_outgoing_edges()
                return {x._uid : x._element_type for x in result}
            else:
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                .get_outgoing_edges(EdgeType(self.graphs[graph_name]._storage, edge_type))
                return {x._uid : x._element_type for x in result}
    
    def v_get_incoming_edges(self, graph_name, uid, type_name, edge_type):
        #return edges
        print('incoming edges')
        print(type_name)
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            if(edge_type is None):
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                    .get_incoming_edges()
                return {x._uid : x._element_type for x in result}    
            else:
                print('Debug')
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                .get_incoming_edges(EdgeType(self.graphs[graph_name]._storage, edge_type))
                print(result)
                return {x._uid : x._element_type for x in result}
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            self.graphs[graph_name] = None
            if(edge_type is None):
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                    .get_incoming_edges()
                return {x._uid : x._element_type for x in result}
            else:
                result = Vertex(uid, type_name, self.graphs[graph_name]._storage)\
                .get_incoming_edges(EdgeType(self.graphs[graph_name]._storage, edge_type))
                return {x._uid : x._element_type for x in result}
    
    def e_get_target(self, graph_name, uid, type_name):
        #return the edge object's target vertices
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            result = Edge(uid, type_name,  self.graphs[graph_name]._storage).get_target()
            return {x._uid : x._element_type for x in result} 
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            result = Edge(uid, type_name,  self.graphs[graph_name]._storage).get_target()
            return {x._uid : x._element_type for x in result} 
    
    def e_get_source(self, graph_name, uid, type_name):
        #return the edge object's source vertex
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            v = Edge(uid, type_name,  self.graphs[graph_name]._storage).get_source()
            return (v._uid, v._element_type)
        else:
            print('Create new graph ' + graph_name)
            self.graphs[graph_name] = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            v = Edge(uid, type_name,  self.graphs[graph_name]._storage).get_source()
            return (v._uid, v._element_type)
    
    def e_add_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        #add a target vertex to edge
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            Edge(uid, type_name,  self.graphs[graph_name]._storage)\
                .add_target(Vertex(vertex_uid, vertex_type, self.graphs[graph_name]._storage))
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            self.graphs[graph_name] = None
            Edge(uid, type_name,  self.graphs[graph_name]._storage)\
                .add_target(Vertex(vertex_uid, vertex_type, self.graphs[graph_name]._storage))
    
    def e_rm_target(self, graph_name, uid, type_name, vertex_uid, vertex_type):
        #remove target vertex from edge
        if(self.graphs.has_key(graph_name)):
            print(graph_name)
            Edge(uid, type_name,  self.graphs[graph_name]._storage)\
                .remove_target(Vertex(vertex_uid, vertex_type, self.graphs[graph_name]._storage))
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            self.graphs[graph_name] = None
            Edge(uid, type_name,  self.graphs[graph_name]._storage)\
                .remove_target(Vertex(vertex_uid, vertex_type, self.graphs[graph_name]._storage))
        
    def remove(self, graph_name, uid, type_name):
        t = type_name.split(':', 1)
        if(self.graphs.has_key(graph_name)):
            #TODO this is a workaround
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).remove()
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).remove()
            else:
                #TODO
                raise Exception()
        else:
            print('Create new graph ' + graph_name)
            #hyperdex = HyperDexGraph(self.hyperdex_ip, self.hyperdex_port, graph_name)
            self.graphs[graph_name] = None
            if(t[0] == 'vertex'):
                return Vertex(uid, t[1], self.graphs[graph_name]._storage).remove()
            elif(t[0] == 'edge'):
                return Edge(uid, t[1], self.graphs[graph_name]._storage).remove()
            else:
                #TODO
                raise Exception()
        
        