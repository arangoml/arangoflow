
class CriticalFailure(Exception):
    """docstring for CriticalFailure"""
    def __init__(self, message):
        super(CriticalFailure, self).__init__()
        self.message = message

    def __str__(self) :
        return self.message

class ArgumentError(Exception) :
    def __init__(self, message, parameters):
        super(ArgumentError, self).__init__()
        self.message = message
        self.parameters = parameters

    def __str__(self) :
        names = []
        for k, v in self.parameters.items() :
            names.append(k)

        return """\n%s. Expected: %s""" % (self.message, ', '.join(names))
