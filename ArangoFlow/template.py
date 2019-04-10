
import grequests
from . import consts
from . import schema
from . import exceptions

        
class FlowProject(object):
    """Representation of a project in ArangodDB.
    @param database : pyArango Database object
    @param project_name : the name of the project used to identify it (does not have to be unique)
    """
    def __init__(self, database, project_name):
        super(FlowProject, self).__init__()
        
        import uuid
       
        self.status = consts.STATUS["PENDING"]
        
        self.database = database
        
        self.name = project_name
        self.processes = []
        self.inputs = []

        self.must_setup = True
        self.uuid = str(uuid.uuid4())
        self.path_uuid = self.uuid

    def register_process(self, process) :
        """registers a process to the project and finds inputs"""
        self.processes.append(process)
        if len(process.ancestors) == 0 :
            self.inputs.append(process)

    def update_status(self, status) :
        """update the project status"""
        self.status = status
        self.arango_doc["status"] = status
        self.arango_doc.patch()

    def notify_error(self, process) :
        """notify the project that a process has ended with a error. If the process as critical it ends the run"""
        if process.rank == consts.RANKS["CRITICAL"] :
            self.update_status(consts.STATUS["ERROR"]) 
            raise exceptions.CriticalFailure("Process: %s, _id : %s, ended with an error" % (process.name, process.arango_doc._id))

    def _db_setup(self):
        """setups the database, creates collections and graph"""
        import time
        
        for col_name in ("Projects", "Processes", "Pipes", "Results", "Monitors") :
            try :
                self.database.createCollection(col_name)
            except Exception as e :
                self.database[col_name].truncate()
        
        try :
            self.database.createGraph("ArangoFlow_graph")
        except Exception as e :
            pass

        self.arango_doc = self.database["Projects"].createDocument()
        
        self.arango_doc.set(
            {
                "start_date" : time.time(),
                "name" : self.name,
                "status": self.status,
                "uuid": self.uuid,
                "path_uuid": self.path_uuid
            }
        )
        self.arango_doc.save()
        self.must_setup = False

    def _build_traverse(self, start_node = None) :
        """creates the run graph in the database"""

        if start_node is None :
            for inp in self.inputs :
                inp._db_create()
                for desc in inp.descendants :
                    desc._db_create()
                    self.database.graphs["ArangoFlow_graph"].link("Pipes", inp.arango_doc._id, desc.arango_doc._id, {"field": desc.ancestors[inp]["field"], "_to_name": inp.name, "_from_name": desc.name } )
                    self._build_traverse(desc)
        else :
            for desc in start_node.descendants :
                desc._db_create()
                e = self.database.graphs["ArangoFlow_graph"].link("Pipes", start_node.arango_doc, desc.arango_doc, {"field": desc.ancestors[start_node]["field"], "_to_name": start_node.name, "_from_name": desc.name })
                self._build_traverse(desc)

    def run(self):
        """build the pipelne graph and runs it"""
        import time
        
        if self.must_setup :
            self._db_setup()

        self.update_status(consts.STATUS["RUNNING"]) 
        
        print("building symbolic graph in arangodb...")
        self._build_traverse()
        print("done")
        
        print("runing the pipeline...")
        for inp in self.inputs :
            inp._run()
        self.arango_doc["end_date"] = time.time()
        self.update_status(consts.STATUS["DONE"]) 
        self.arango_doc.patch()
        print("done")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        """only updates end time but could handle stuff like pending / unfinished jobs and rollbacks"""
        import time
        self.arango_doc["end_date"] = time.time()
        self.arango_doc.patch()

class Node(object):
    """docstring for Node"""
    def __init__(self):
        super(Node, self).__init__()

class ProcessPlaceholderField(Node):
    """docstring for  ProcessPlaceholderField"""
    def __init__(self, process, field):
        super(ProcessPlaceholderField, self).__init__()
        self.process = process
        self.field = field

    @property
    def result(self):
        self._result = self.process()[self.field]
        return self._result
    
    def __getattr__(self, k) :
        proc = super(ProcessPlaceholderField, self).__getattribute__('process')
        return getattr(proc, k)

    def __call__() :
        return self.result

class ProcessPlaceholderTick(Node):
    """docstring for  ProcessPlaceholderTick"""
    def __init__(self, process, field):
        super(ProcessPlaceholderTick, self).__init__()
        self.process = process
        self.field = field
    
    def __getattr__(self, k) :
        proc = super(ProcessPlaceholderTick, self).__getattribute__('process')
        return getattr(proc, k)

class MetaProcess(Node):
    """Processes are atomic routines that take an arbitrary number of inputs and return a single outputs
    All processes received as arguments to __init__ are considered ancestors. A process will not run until
    all it's ancestors have successfully finished. All other arguments are considered parameters and will
    be saved in the databse for future reference.
    To use ArangoFlow, users will have to create their own processes by inheriting from this class. The must
    at least define the run() function. The end results of a process are stored in self.result
    """
        
    def __new__(cls, *args, **kwargs) :
        """Analyse the arguments passed to __init__ finds ancestors (other processes needed for the conputation) and parameters (anything else) """
        import inspect
        import hashlib
        
        def parse_argument(obj, name, value, ancestors, parameters) :
            if isinstance(value, Node) :
                if isinstance(value, ProcessPlaceholderField) or isinstance(value, ProcessPlaceholderTick) :
                    ancestors[value.process] = {"status": value.process.status, "argument_name": name, 'field': value.field}
                    value.process.register_descendant(obj)
                else :
                    ancestors[value] = {"status": value.status, "argument_name": name, 'field': None}
                    value.register_descendant(obj)
            elif not isinstance(value, FlowProject) :
                parameters[name] = value
            
        def parse_arguments(obj, name, value, ancestors, parameters) :
            from collections.abc import Iterable

            if type(value) is dict :
                for k, v in value.items() :
                    parse_arguments(obj, "%s.%s" % (name, k), v, ancestors, parameters)
            elif isinstance(value, Iterable) :
                for i, v in enumerate(value) :
                    parse_arguments(obj, "%s[%s]" % (name, i), v, ancestors, parameters)
            else :
                parse_argument(obj, name, value, ancestors, parameters)

        obj = super(MetaProcess, cls).__new__(cls)
        sig = inspect.signature(cls.__init__)

        parameters = {}
        ancestors = {}
        provided_args = set(["self"]) #one foe self

        for k, v in sig.parameters.items() :
            if v.default is not inspect.Parameter.empty :
                provided_args.add(k)

        for k, v in kwargs.items() :
            provided_args.add(k)
            if k not in ["self", "project", "collection_name", "rank", "checkpoint"] :
                parse_arguments(obj, k, v, ancestors, parameters)
                # if isinstance(v, Node) and k != "self" :
                #     if isinstance(v, ProcessPlaceholderField) or isinstance(v, ProcessPlaceholderTick) :
                #         ancestors[v.process] = {"status": v.process.status, "argument_name": k, 'field': v.field}
                #         v.process.register_descendant(obj)
                #     else :
                #         ancestors[v] = {"status": v.status, "argument_name": k, 'field': None}
                #         v.register_descendant(obj)
                # elif not isinstance(v, FlowProject) :
                #     parameters[k] = v

        frame = inspect.currentframe()
        frame_args = inspect.getargvalues(frame).locals["args"]
        if len(frame_args) > 0 :
            for i, kv in enumerate(sig.parameters.items()) :
                provided_args.add(kv[0])
                if kv[0] in kwargs or kv[0] == "kwargs":
                    break #args finished, entering into kwargs
                if i > 0 : #skip self argument
                    j = i-1
                    # print(kv[1].name, frame_args[j])
                    parse_arguments(obj, kv[0], frame_args[j], ancestors, parameters)
                    # if isinstance(frame_args[j], MetaProcess) :
                    #     if isinstance(frame_args[j], ProcessPlaceholderField) or isinstance(frame_args[j], ProcessPlaceholderTick) :
                    #         ancestors[frame_args[j].process] = {"status": frame_args[j].process.status, "argument_name": kv[0], "field": frame_args[j].field}
                    #         frame_args[j].process.register_descendant(obj)
                    #     else :
                    #         ancestors[frame_args[j]] = {"status": frame_args[j].status, "argument_name": kv[0], "field": None}
                    #         frame_args[j].register_descendant(obj)
                    # elif not isinstance(frame_args[j], FlowProject) :
                    #     parameters[kv[0]] = frame_args[j]

        if len(sig.parameters) != len(provided_args) :
            raise exceptions.ArgumentError(list(sig.parameters.keys()), list(provided_args))


        obj.parameters = parameters
        obj.ancestors = ancestors

        src = []
        obj.uuid = None
        for fct_name in dir(obj):
            fct = getattr(obj, fct_name)
            if callable(fct) :
                try:
                    src.append( inspect.getsource(fct) )
                except TypeError as e:
                    src.append(fct_name)
    
        obj.uuid = hashlib.md5(''.join(src).encode('utf-8')).hexdigest()
        
        return obj

    def __init__(self, project, collection_name, rank = consts.RANKS["CRITICAL"], checkpoint=True, **kwargs):
        """The first argument must allways be the project. A precess with critical rank will end the run. A process with no critical rank should only end its branch (not implemented, see: recieve_ancestor_join) """
        super(MetaProcess, self).__init__()
        
        self.collection_name = collection_name
        self.rank = rank
        self.status = consts.STATUS["PENDING"]
        self.checkpoint = checkpoint
        
        self.project = project

        self.name = self.__class__.__name__
        
        self.ancestors_ready = set()
        self.ancestors_finished = set()

        if not hasattr(self, "descendants") :
           self.descendants = []
        self.result = None
        
        self.project.register_process(self)
        
        self.must_setup = True
        self.monitors = []

    @property
    def path_uuid(self):
        parents = ','.join([a.path_uuid for a in self.ancestors ])
        path_uuid = "%s(%s)" % (self.uuid, parents)
        return path_uuid
    
    def update_critical_rank(self, rank) :
        """Update the rank of impotance of the process"""
        self.rank = rank

    def _db_create(self) :
        """create the process in the database"""
        import time
        import inspect
        
        if not self.must_setup :
            return True

        self.arango_doc = self.project.database[self.collection_name].createDocument()
    
        if not self.__class__.__doc__ :
           doc = ""
        else :
           doc = inspect.cleandoc(self.__class__.__doc__)

        self.arango_doc.set(
            {
                "start_date" : None,
                "project": self.project.arango_doc._id,
                "status": self.status,
                "name": self.name,
                "rank": self.rank,
                "parameters" : self.parameters,
                "checkpoint": self.checkpoint,
                "uuid": self.uuid,
                "path_uuid": self.path_uuid,
                "description" : doc
            }
        )
        self.arango_doc.save()

        self.must_setup = False

    def register_descendant(self, process) :
        """Register a process as being a descendent of self"""
        self.descendants.append(process)
        if isinstance(process, Monitor) :
            self.monitors.append(process)

    def join(self) :
        """joins decendents at the end of the run"""
        for d in self.descendants :
            d.recieve_ancestor_join(self)
    
    def tick(self, tick_field, value) :
        assert type(tick_field) is str
        for d in self.monitors :
            d.recieve_tick_notification(value, self, tick_field)
    
    def ticks(self, field) :
        return ProcessPlaceholderTick(self, field)

    def recieve_ancestor_join(self, process) :
        """receive an end of run notification from an ancestor. If process has at least one of it's ancestors termiate with a error, it will raise a RuntimeError"""
        self.ancestors[process]["status"] = process.status
        self.ancestors_finished.add(self.ancestors[process]["argument_name"])
        
        if process.status == consts.STATUS["DONE"] :
            self.ancestors_ready.add(self.ancestors[process]["argument_name"])
        
        if len(self.ancestors_ready) == len(self.ancestors) :
            self._run()
        elif len(self.ancestors_finished) == len(self.ancestors) :
            raise RuntimeError("Process upward of self finished with errors: %s" % (self.ancestors_finished - self.ancestors_ready) )

    def update_status(self, status) :
        """update status in the database"""
        self.status = status
        self.arango_doc["status"]= status
        self.arango_doc.patch()

    def _save_checkpoint(self) :
        """TODO: save the resulting data on disk so it can reloaded if necessary"""
        pass

    def _run(self) :
        """private run function, takes care notifications and updating status"""
        import time
        def update_start_date() :
            self.arango_doc["start_date"] = time.time()
            self.arango_doc.patch()
        
        def update_end_date() :
            self.arango_doc["end_date"] = time.time()
            self.arango_doc.patch()

        update_start_date()
        try:
            self.result = self.run()
        except Exception as e:
            self.update_status(consts.STATUS["ERROR"])
            update_end_date()
            self.project.notify_error(self)
        else :
            self.update_status(consts.STATUS["DONE"])
            if self.checkpoint:
                self._save_checkpoint()

            update_end_date()
            self.join()

    def run(self) :
        """the function that users must redefine, must runs and return the output"""
        raise NotImplementedError("Must be implemented in child")

    def __getitem__(self, k) :
        return ProcessPlaceholderField(self, k)

    def __call__(self) :
        return self.result

class Process(MetaProcess):
    """docstring for Process"""
    def __init__(self, project, **kwargs):
        super(Process, self).__init__(project = project, collection_name = "Processes", **kwargs)

class Monitor(MetaProcess):
    """docstring for Monitor"""

    def __init__(self, project, tick_input, rank=consts.RANKS["CRITICAL"]):
        super(Monitor, self).__init__(project = project, collection_name = "Monitors", rank=rank, checkpoint = False)
        from collections.abc import Iterable
        
        self.tick_input = set()
        if isinstance(tick_input, Iterable) :
            for v in tick_input :
                self.tick_input.add( (v.process.uuid, v.field) )
        else :
            try :
                self.tick_input.add((tick_input.process.uuid, tick_input.field))
            except :
                raise ArgumentError("object %s is not a valid instance of Process" % tick_input)

    def recieve_tick_notification(self, value, process, tick_field) :
        if (process.uuid, tick_field) in self.tick_input :
            self._tick_run(process, value)

    def _tick_run(self, process, value) :
        import time

        self.update_status(consts.STATUS["RUNNING"])

        tick_dct = {
            "start_date": time.time(),
            "end_date": None,
            "process_name": process.name,
            "process_id": process.arango_doc["_id"],
            "process_uuid": process.arango_doc["uuid"],
            "process_path_uuid": process.arango_doc["path_uuid"],
            "status": consts.STATUS["RUNNING"]
        }

        try:
            self.tick_run(process, value)
        except Exception as e:
            # print(e.message)
            tick_dct["status"] = consts.STATUS["ERROR"]
            self.project.notify_error(self)
        else :
            tick_dct["status"] = consts.STATUS["DONE"]
        
        tick_dct["end_date"] = time.time()
        self.update_status(consts.STATUS["PENDING"])
        self.arango_doc["ticks"].append(tick_dct)
        self.arango_doc["ticks"] = self.arango_doc["ticks"]
        self.arango_doc.patch()

    def tick_run(self, process, input_value) :
        raise NotImplementedError("Must be implemented in child")

    def run(self) :
        self.update_status(consts.STATUS["DONE"])
  
class Result(MetaProcess):
    """A result is a process with (usually) low critical rank that takes care of fromating results, saving in the database or serializaing them to disk"""
    
    def __init__(self, project, rank = consts.RANKS["NOT_CRITICAL"], checkpoint=False, **kwargs):
        super(Result, self).__init__(project = project, rank = rank, collection_name = "Result", checkpoint=checkpoint **kwargs)

