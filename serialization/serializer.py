"""
.. module:: serializer.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
import cPickle


def serialize(value):
    return cPickle.dumps(value, cPickle.HIGHEST_PROTOCOL)

def deserialize(value):
    return cPickle.loads(value)