import Pyro4

#Weimar settings
WEIMAR_PORT_INSIDE = 2522
WEIMAR_ADDRESS_INSIDE = '192.168.100.26'

WEIMAR_PORT_OUTSIDE = 2523
WEIMAR_ADDRESS_OUTSIDE = '192.168.100.26'

Pyro4.config.SERVERTYPE="thread"
Pyro4.config.THREADING2=True
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.HMAC_KEY='weimar-graphstore'
Pyro4.config.COMPRESSION = True