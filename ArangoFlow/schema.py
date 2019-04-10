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
        "status": Field(validators = [VAL.Enumeration(consts.STATUS.values())], default = consts.STATUS["PENDING"]),
        "uuid": Field(validators = [VAL.NotNull()]),
        "path_uuid": Field(validators = [VAL.NotNull()])
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
        "status": Field(validators = [VAL.Enumeration(consts.STATUS.values())], default = consts.STATUS["PENDING"]),
        "rank": Field(validators = [VAL.Enumeration(consts.RANKS.values())]),
        "name" : Field(validators = [VAL.NotNull()]),
        "checkpoint" : Field(validators = [VAL.Bool(), VAL.NotNull()], default=True),
        "parameters" : {},
        "uuid": Field(validators = [VAL.NotNull()]),    
        "path_uuid": Field(validators = [VAL.NotNull()]),
        "description": Field(default=""),
    }

class Monitors(Collection):
    _validation = {
        "on_save" : True,
        "on_set" : True,
        "allow_foreign_fields" : True
    }

    _fields = {
        "start_date" : Field(),
        "end_date" : Field(),
        "project" : Field(validators = [VAL.NotNull()]),
        "status": Field(validators = [VAL.Enumeration(consts.STATUS.values())], default = consts.STATUS["PENDING"]),
        "rank": Field(validators = [VAL.Enumeration(consts.RANKS.values())]),
        "name" : Field(validators = [VAL.NotNull()]),
        "checkpoint" : Field(validators = [VAL.Bool(), VAL.NotNull()], default=True),
        "parameters" : {},
        "uuid": Field(validators = [VAL.NotNull()]),
        "path_uuid": Field(validators = [VAL.NotNull()]),
        "description": Field(default=""),
        "ticks": Field(default = [])
    }

class Results(Collection):
    _validation = {
        "on_save" : True,
        "on_set" : True,
        "allow_foreign_fields" : True
    }

    _fields = {
        "start_date" : Field(),
        "end_date" : Field(),
        "project" : Field(validators = [VAL.NotNull()]),
        "status": Field(validators = [VAL.Enumeration(consts.STATUS.values())], default = consts.STATUS["PENDING"]),
        "rank": Field(validators = [VAL.Enumeration(consts.RANKS.values())]),
        "name" : Field(validators = [VAL.NotNull()]),
        "parameters" : {},
        "uuid": Field(validators = [VAL.NotNull()]),
        "path_uuid": Field(validators = [VAL.NotNull()]),
        "description": Field(default="")
    }

        
class Pipes(Edges) :
    
    _validation = {
        "on_save" : False,
        "on_set" : False,
        "allow_foreign_fields" : True
    }

    _fields = {
        "field": Field(),
        "_to_name": Field(validators = [VAL.NotNull()]),
        "_from_name": Field(validators = [VAL.NotNull()]),
    }

class ArangoFlow_graph(GR.Graph):
    _edgeDefinitions = (
        GR.EdgeDefinition("Pipes", fromCollections = ["Processes", "Monitors", "Results"], toCollections = ["Processes", "Monitors", "Results"]),
    )
