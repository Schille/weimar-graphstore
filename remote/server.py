'''
Created on Mar 17, 2014

@author: mschilonka
'''
import Pyro4
from multiprocessing import Process, Manager
import time

import config
import signal
import sys

import multiprocessing as mp
from remote.serverprocesses import ClusterManager
from remote.weimargraphserver import WeimarGraphServer
    

    
def start_server(hyperdex_ip, hyperdex_port):
    print('[Info] Starting Weimar...')
    #start the internal coordinator in order to dispatch workload
    print('=== Weimar ClusterManager ===')
    weimar_cluster = ClusterManager(hyperdex_ip, hyperdex_port)
    #this is the pool of workers available to this instance of Weimar
    worker_pool = weimar_cluster.get_worker_pool()
    #a graph server, for external clients
    print('=== Weimar GraphServer ===')
    graph_server = WeimarGraphServer(worker_pool)
    print('[Info] Starting Weimar...Successful')
    
    def signal_handler(signum, frame):
        print('\n[Info] Shutting down Weimar...')
        graph_server.shutdown()
        graph_server.join(10)
        weimar_cluster.shutdown()
        weimar_cluster.join(10)
        print('[Info] Shutting down Weimar...Successful')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    weimar_cluster.join()
        
                 
        

