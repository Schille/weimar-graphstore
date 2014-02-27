"""
.. module:: graphexception.py
   :platform: Linux

.. moduleauthor:: Michael Schilonka <michael@schilonka.de>


"""
class ElementNotFoundException(Exception):

    def __init__(self,uid, element_type):
        self.value = 'Element {0} with uid {1} not found.'.format(element_type, uid)
        
    def __str__(self, *args, **kwargs):
        return self.value

class TypeNotFoundException(Exception):
    
    def __init__(self,element_type):
        self.value = 'Type {0} with not found.'.format(element_type)
        
    def __str__(self, *args, **kwargs):
        return self.value1
    