
import multiprocessing as mp
import config, Pyro4, time, signal
from remote.workerprocess import WorkerProcess
from remote.server_utils import HyperDexProperties, WorkerRegistry


class NameServer(mp.Process):
    
    def __init__(self, ns_ip, ns_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(mp.Process, self).__init__(name='weimar-nameserver')
        self.address = ns_ip
        self.port = ns_port
        self.start()
        time.sleep(1)
        
    def run(self):
        self.uri, self.nsdaemon, self.bc = Pyro4.naming.startNSloop\
        (host=self.address, port=self.port, enableBroadcast=False)
        
    
    def shutdown(self):
        self.nsdaemon.close()
        time.sleep(1)
        self.terminate()

class PropertyServer(mp.Process):
    
    def __init__(self, hyperdex_ip, hyperdex_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(PropertyServer, self).__init__(name='weimar-propertyprovider')
        self.daemon = Pyro4.Daemon(host=config.WEIMAR_ADDRESS_INSIDE)
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        self.hyperdex = HyperDexProperties(hyperdex_ip, hyperdex_port)
        self.registry = WorkerRegistry()
        properties_uri = self.daemon.register(self.hyperdex)
        registry_uri = self.daemon.register(self.registry)
        self.ns.register('hyperdex.properties', properties_uri)
        self.ns.register('weimar.worker.registry', registry_uri)
        self.start()
        time.sleep(1)

    def run(self, hyperdex_ip, hyperdex_port):
        self.daemon.requestLoop()

    def shutdown(self):
        self.daemon.close()
        time.sleep(1)
        self.terminate()

class WorkerRegister(mp.Process):
    
    def __init__(self, worker_p):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(mp.Process, self).__init__(name='weimar-workerregister')
        self.worker_p = worker_p
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        registry_uri = self.ns.lookup('weimar.worker.registry')
        self.registry = Pyro4.Proxy(registry_uri) 
        self.known_worker = []
        self.online = 0
        self.start()
        
    def run(self):
        while True:
            if(self.online != self.registry.get_worker_count()):
                online = self.registry.get_worker_count()
                print('Workers online: {}'.format(online))
                all_worker = self.registry.get_worker_names()
        
                print(str(all_worker))
                print(str(self.known_worker))
                for worker in all_worker:
                    if(worker not in self.known_worker):
                        print('Adding worker...')
                        #proxy = Pyro4.Proxy(ns.lookup('weimar.worker.{}'.format(worker)))
                        self.worker_p.put(WorkerProcess(worker))
                        #worker_pool.task_done()
                        self.known_worker.append(worker)
            
                for worker in self.known_worker:
                    if(worker not in all_worker):
                        self.known_worker.remove(worker)
                        print('Removing worker...')
                        for i in xrange(0, self.worker_p.qsize()):
                            try:
                                workerp = self.worker_p.get(False)
                            except:
                                continue
                            if(workerp.name != worker):
                                self.worker_p.put(workerp)
        time.sleep(5)
        
    def shutdown(self):
        self.ns.close()
        self.terminate()

class ClusterServer(mp.Process):
    
    def __init__(self, worker_pool,  hyperdex_ip, hyperdex_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(mp.Process, self).__init__(name='weimar-clusterserver')
        #start the cluster name server
        self._i_nssvr = NameServer(config.WEIMAR_ADDRESS_INSIDE, config.WEIMAR_PORT_INSIDE)

        #start the properties provider
        self.prop_server = PropertyServer(hyperdex_ip, hyperdex_port)
        #create a queue for all worker process attached to this server instance
        self.worker_pool = worker_pool
        self.workp_register = WorkerRegister(self.worker_pool)
        self.start()
    
    def run(self):
        while(True):
            time.sleep(5)
    
    
    def shutdown(self):
        print('Shutting down ClusterServer...')
        self.prop_server.shutdown()
        self.workp_register.shutdown()
        self._i_nssvr.shutdown()
        print('Shutting down ClusterServer...Done! ')
        self.terminate()

        
        
        
       
        
        