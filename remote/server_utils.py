
import Pyro4, config

class HyperDexProperties(object):
    
    def __init__(self, hyperdex_ip, hyperdex_port):
        self.hyperdex_ip = hyperdex_ip
        self.hyperdex_port = hyperdex_port

    def get_ip(self):
        return self.hyperdex_ip
    
    def get_port(self):
        return self.hyperdex_port

class WorkerRegistry(object):
    
    def __init__(self):
        self._dict = {}
        self._ns = Pyro4.naming.locateNS(host=config.WEIMAR_ADDRESS_INSIDE, port=config.WEIMAR_PORT_INSIDE)
    
    def get_worker_count(self):
        return len(self._dict)
    
    def get_worker_handle(self, worker_name = None):
        if(worker_name is not None):
            if(worker_name in self._dict):
                if(self._dict[worker_name] is None):
                    self._dict[worker_name] = Pyro4.Proxy(self._ns.lookup('weimar.worker.{}'.format(worker_name)))
                return self._dict[worker_name]
        else:
            result = []
            for worker in self.get_worker_names():
                result.append(self.get_worker_handle(worker))
            return result
        
    def get_worker_names(self):
        return self._dict.keys()                
    
    def register(self):
        name = 'Worker{}'.format(len(self._dict))
        self._dict[name] = None
        return name
    
    def unregister(self, worker):
        self._dict.pop(worker, None)