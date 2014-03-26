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
from remote.serverprocesses import ClusterServer
from remote.weimargraphserver import WeimarGraphServer
    

    
def start_server(hyperdex_ip, hyperdex_port):
    print('Starting Weimar...')
    #this is the pool of workers available to this instance of Weimar
    worker_pool = mp.Manager().Queue()
    #start the internal coordinator in order to dispatch workload
    weimar_cluster = ClusterServer(worker_pool, hyperdex_ip, hyperdex_port)
    #a graph server, for external clients
    graph_server = WeimarGraphServer(worker_pool)
    print('Starting Weimar...Done')
    
    def signal_handler(signum, frame):
        print('\nShutting down Weimar...')
        graph_server.shutdown()
        graph_server.join(10)
        weimar_cluster.shutdown()
        weimar_cluster.shutdown(10)
        print('Shutting down Weimar...Done')
        sys.exit()
    
    signal.signal(signal.SIGINT, signal_handler)
    while(True):
        time.sleep(5)
                 
        

