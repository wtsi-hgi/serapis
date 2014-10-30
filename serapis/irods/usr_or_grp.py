'''
Created on Oct 28, 2014

@author: ic4

'''

class iRODSUserOrGroup(object):
    
    @staticmethod
    def build(usr_or_grp, zone):
        return usr_or_grp+"#"+zone
