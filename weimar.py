'''
Created on Mar 17, 2014

@author: mschilonka
'''
import argparse, sys
from remote import server as Server
from remote import worker as Worker


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--worker", help="Starts a weimar worker instance.", action="store_true")
    parser.add_argument("-t", "--threads",type=int, dest='threads', help="The number of threads running in one a worker (Default=3).")
    parser.add_argument("-s", "--server", help="Starts a weimar graph server.", action="store_true")
    parser.add_argument("-i", "--hyperdex-ip",type=str ,dest='hyperdex_ip',  help='The HyperDex coordinator IP address. Must be specified if a server is started.')
    parser.add_argument("-p", "--hyperdex-port",type=int ,dest='hyperdex_port', help="The HyperDex coordinator port number. Must be specified if a server is started.")
    args = parser.parse_args()

    if args.worker:
        if(args.threads is None):
            args.threads = 3
        Worker.start_worker(args.threads)
    elif args.server:
        if(args.hyperdex_ip is None or args.hyperdex_port is None):
            print('When starting a Weimar server, please specify the HyperDex\'s coordinators ip and port.')
            parser.print_help()
            sys.exit(1)
        if(args.threads is not None):
            print('--threads only refers to a worker process and will be omitted.')
        Server.start_server(args.hyperdex_ip, args.hyperdex_port)