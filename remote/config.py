import Pyro4

#Weimar settings
WEIMAR_PORT_INSIDE = 2511
WEIMAR_ADDRESS_INSIDE = '192.168.100.26'

WEIMAR_PORT_OUTSIDE = 2511
WEIMAR_ADDRESS_OUTSIDE = '141.72.5.121'

Pyro4.config.SERVERTYPE="thread"
Pyro4.config.THREADING2=True
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.HMAC_KEY='weimar-graphstore'
#Pyro4.config.COMMTIMEOUT=10.5
Pyro4.config.COMPRESSION = True