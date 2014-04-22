
import multiprocessing as mp
import config, Pyro4, time, signal
from remote.workerprocess import WorkerProcess
from remote.server_utils import HyperDexProperties, WorkerRegistry


class NameServer(mp.Process):
    
    def __init__(self, ns_ip, ns_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(NameServer, self).__init__(name='weimar-nameserver-{}'.format(ns_port))
        self.address = ns_ip
        self.port = ns_port
        self._running = mp.Value('i', 1)
        self.start()
        time.sleep(2)
        
    def run(self):
        print('[Info] Starting: Name server on {}:{}'.format(self.address, self.port))
        self.uri, self.nsdaemon, self.bc = Pyro4.naming.startNS\
        (host=self.address, port=self.port, enableBroadcast=False)
        self.nsdaemon.requestLoop(loopCondition=lambda:self._running.value)
        #shutdown was issued
        self.nsdaemon.close()
    
    def shutdown(self):
        self._running.value = 0
        time.sleep(1)
        self.terminate()

class PropertyServer(mp.Process):
    
    def __init__(self, hyperdex_ip, hyperdex_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(PropertyServer, self).__init__(name='weimar-propertyprovider')
        self.hyperdex_ip = hyperdex_ip
        self.hyperdex_port = hyperdex_port
        self._running = mp.Value('i', 1)
        self.start()
        time.sleep(1)


    def run(self):
        self.daemon = Pyro4.core.Daemon(host=config.WEIMAR_ADDRESS_INSIDE)
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        self.hyperdex = HyperDexProperties(self.hyperdex_ip, self.hyperdex_port)
        self.registry = WorkerRegistry()
        properties_uri = self.daemon.register(self.hyperdex)
        registry_uri = self.daemon.register(self.registry)
        self.ns.register('hyperdex.properties', properties_uri)
        self.ns.register('weimar.worker.registry', registry_uri)
        print('[Info] Starting: weimar-propertyprovider')
        self.daemon.requestLoop(loopCondition=lambda:self._running.value)
        #shutdown was issued
        self.nsdaemon.close()

    def shutdown(self):
        self._running.value = 0
        time.sleep(1)
        self.terminate()

class WorkerRegister(mp.Process):
    
    def __init__(self, worker_p):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(WorkerRegister, self).__init__(name='weimar-workerregister')
        self.worker_p = worker_p
        self.ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
        self.ns.ping()
        self.known_worker = []
        self.online = mp.Value('i', 0)
        self.start()
        time.sleep(1)
        
    def run(self):
        print('[Info] Starting: weimar-workerregister')
        registry_uri = self.ns.lookup('weimar.worker.registry')
        self.registry = Pyro4.core.Proxy(registry_uri)
        while True:
            time.sleep(5)
            if(self.online.value != self.registry.get_worker_count()):
                self.online.value = self.registry.get_worker_count()
                print('[Info] Workers online: {}'.format(self.online.value))
                all_worker = self.registry.get_worker_names()
                for worker in all_worker:
                    if(worker not in self.known_worker):
                        #proxy = Pyro4.Proxy(ns.lookup('weimar.worker.{}'.format(worker)))
                        self.worker_p.put(WorkerProcess(worker))
                        #worker_pool.task_done()
                        self.known_worker.append(worker)
                
                
                def _remove_worker(worker):
                    if(worker not in all_worker):
                        for i in xrange(0, self.worker_p.qsize()):
                            try:
                                workerp = self.worker_p.get(False)
                            except:
                                pass
                            if(workerp.name != worker):
                                self.worker_p.put(workerp)
                            else:
                                print('[Warn] removing worker: ' + workerp.name)
                    else:
                        return worker
                self.known_worker = filter(_remove_worker, self.known_worker)
                print('[Debug] Worker queue: ' + str(self.worker_p.qsize()))
        
    def shutdown(self):
        #shut down all attached worker processes
        if(self.worker_p.qsize() > 0):
            print('[Info] Shutting down all worker processes...') 
            for i in xrange(0, self.worker_p.qsize()):
                try:
                    workerp = self.worker_p.get(False)
                    workerp.shutdown(1000)
                    #grant some time to send shutdown signal properly
                    time.sleep(2)
                except:
                    time.sleep(2)
            time.sleep(5)
        self.terminate()

class ClusterManager(mp.Process):
    
    def __init__(self, hyperdex_ip, hyperdex_port):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        super(ClusterManager, self).__init__(name='weimar-clustermanager')
        #worker pool
        self.worker_pool = mp.Manager().Queue()
        
        #start the cluster name server
        self._i_nssvr = NameServer(config.WEIMAR_ADDRESS_INSIDE, config.WEIMAR_PORT_INSIDE)

        #start the properties provider
        self.prop_server = PropertyServer(hyperdex_ip, hyperdex_port)
        #create a queue for all worker process attached to this server instance
        self.workp_register = WorkerRegister(self.worker_pool)
        self.start()
    
    def run(self):
        #TODO collect data/statistics
        while(True):
            time.sleep(10)
    
    
    def shutdown(self):
        print('[Info] Shutting down ClusterManager...')
        self.workp_register.shutdown()
        time.sleep(1)
        self.prop_server.shutdown()
        time.sleep(1)
        self._i_nssvr.shutdown()
        print('[Info] Shutting down ClusterManager...Done ')
        self.terminate()

    def get_worker_pool(self):
        return self.worker_pool
        
        
       
        
        