import abc

class ZKClient(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_data(self, path):
        return

    @abc.abstractmethod
    def get_children(self, path):
        return
