from pyArango.collection import Collection, Edges, Field
import pyArango.validation as VAL
import pyArango.graph as GR

from . import consts

class Projects(Collection) :
    _validation = {
        "on_save" : True,
        "on_set" : True,
        "allow_foreign_fields" : True
    }

    _fields = {
        "start_date" : Field(validators = [VAL.NotNull()]),
        "end_date" : Field(),
        "name" : Field(validators = [VAL.NotNull()]),
        "status": Field(validators = [VAL.Enumeration([consts.STATUS_PENDING, consts.STATUS_DONE, consts.STATUS_ERROR, consts.STATUS_WORKING])], default = consts.STATUS_PENDING),
        # "description": Field(validators = [VAL.NotNull()]),
    }

class Processes(Collection) :
    
    _validation = {
        "on_save" : True,
        "on_set" : True,
        "allow_foreign_fields" : True
    }

    _fields = {
        "start_date" : Field(),
        "end_date" : Field(),
        "project" : Field(validators = [VAL.NotNull()]),
        "status": Field(validators = [VAL.Enumeration([consts.STATUS_PENDING, consts.STATUS_DONE, consts.STATUS_ERROR, consts.STATUS_WORKING])], default = consts.STATUS_PENDING),
        "name" : Field(validators = [VAL.NotNull()]),
        "parameters" : {},
        "description": Field(validators = [VAL.NotNull()])
    }

class Results(Collection):
    """docstring for Result"""
    _validation = {
        "on_save" : True,
        "on_set" : True,
        "allow_foreign_fields" : True
    }

    _fields = {
        "start_date" : Field(),
        "end_date" : Field(),
        "project" : Field(validators = [VAL.NotNull()]),
        "status": Field(validators = [VAL.Enumeration([consts.STATUS_PENDING, consts.STATUS_DONE, consts.STATUS_ERROR, consts.STATUS_WORKING])], default = consts.STATUS_PENDING),
        "name" : Field(validators = [VAL.NotNull()]),
        "parameters" : {},
        "description": Field(validators = [VAL.NotNull()])
    }

        
class Pipes(Edges) :
    
    _validation = {
        "on_save" : False,
        "on_set" : False,
        "allow_foreign_fields" : True
    }

    _fields = {}

class ArangoFlow_graph(GR.Graph):
    _edgeDefinitions = (
        GR.EdgeDefinition("Pipes", fromCollections = ["Processes"], toCollections = ["Processes", "Results"]),
    )
