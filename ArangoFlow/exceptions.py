
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

class ArgumentError(Exception) :
    def __init__(self, expected_arguments, got_argumets):
        super(ArgumentError, self).__init__()
        self.expected_arguments = expected_arguments
        self.got_argumets = got_argumets

    def __str__(self) :
        msg = "Expected {nb_expected} arguments ({exp_arguments}), got {nb_got} ({got_args})".format(
                nb_expected = len(self.expected_arguments),
                exp_arguments = ', '.join(self.expected_arguments),
                nb_got = len(self.got_argumets),
                got_args = ', '.join(self.got_argumets)
            )

        return msg
