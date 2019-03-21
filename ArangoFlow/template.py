
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
       
        self.status = consts["STATUS"]["PENDING"]
        
        self.database = database
        
        self.name = project_name
        self.processes = []
        self.inputs = []

        self.must_setup = True

    def register_process(self, process) :
        """registers a process to the project and finds inputs"""
        self.processes.append(process)
        if len(process.ancestors) == 0 :
            self.inputs.append(process)

    def update_status(self, status) :
        """update the project status"""
        self.status = status
        self.arango_doc["status"]= status
        self.arango_doc.patch()

    def notify_error(self, process) :
        """notify the project that a process has ended with a error. If the process as critical it ends the run"""
        if process.rank == consts.RANKS["CRITICAL"] :
            self.update_status(consts["STATUS"]["ERROR"]) 
            raise exceptions.CriticalFailure("Process: %s, _id : %s, ended with an error" % (process.name, process.arango_doc._id))

    def _db_setup(self):
        """setups the database, creates collections and graph"""
        import time

        for col_name in ("Projects", "Processes", "Pipes", "Results") :
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
            }
        )
        self.arango_doc.save()
        self.must_setup = False

    def _build_traverse(self, start_node = None) :
        """creates the run graph in the database"""
        if self.must_setup :
            self._db_setup()

        if start_node is None :
            for inp in self.inputs :
                inp._db_create()
                for desc in inp.descendants :
                    desc._db_create()
                    self.database.graphs["ArangoFlow_graph"].link("Pipes", inp.arango_doc._id, desc.arango_doc._id, {})
                    self._build_traverse(desc)
        else :
            for desc in start_node.descendants :
                desc._db_create()
                self.database.graphs["ArangoFlow_graph"].link("Pipes", start_node.arango_doc, desc.arango_doc, {})
                self._build_traverse(desc)

    def run(self):
        """build the pipelne graph and runs it"""
        import time

        self.update_status(consts["STATUS"]["RUNNING"]) 
        
        print("building symbolic graph in arangodb...")
        self._build_traverse()
        print("done")
        
        print("runing the pipeline...")
        for inp in self.inputs :
            inp._run()
        self.arango_doc["end_date"] = time.time()
        self.update_status(consts["STATUS"]["DONE"]) 
        self.arango_doc.patch()
        print("done")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        """only updates end time but could handle stuff like pending / unfinished jobs and rollbacks"""
        import time
        self.arango_doc["end_date"] = time.time()
        self.arango_doc.patch()


class Process(object):
    """Processes are atomic routines that take an arbitrary number of inputs and return a single outputs
    All processes received as arguments to __init__ are considered ancestors. A process will not run until
    all it's ancestors have successfully finished. All other arguments are considered parameteres and will
    be saved in the databse for future reference.
    To use ArangoFlow, users will have to create their own processes by inheriting from this class. The must
    at least define the run() function. The end results of a process are stored in self.result
    """
    def __new__(cls, *args, **kwargs) :
        """Analyse the arguments passed to __init__ finds ancestors (other processes needed for the conputation) and parameters (anything else) """
        import inspect
        obj = super(Process, cls).__new__(cls)
        sig = inspect.signature(cls.__init__)
        if len(sig.parameters) != (len(args) + len(kwargs) + 1) : # +1 for self
            raise exceptions.ArgumentError("Expected %s arguments, got %s" % (len(sig.parameters), len(args) + len(kwargs) +1 ), sig.parameters )

        parameters = {}
        ancestors = {}
    
        for k, v in kwargs.items() :
            if isinstance(v, Process) :
                ancestors[v] = {"status": v.status, "argument_name": k}
                v.register_descendant(obj)
            elif not isinstance(v, FlowProject) :
                parameters[k] = v

        frame = inspect.currentframe()
        frame_args = inspect.getargvalues(frame).locals["args"]
        if len(frame_args) > 0 :
            for i, kv in enumerate(sig.parameters.items()) :
                if i > 0 :
                    j = i-1
                    if kv[0] in kwargs :
                        break #args finished, entering into kwargs
                    
                    if isinstance(frame_args[j], Process) :
                        ancestors[frame_args[j]] = {"status": frame_args[j].status, "argument_name": kv[0]}
                        frame_args[j].register_descendant(obj)
                    elif not isinstance(frame_args[j], FlowProject) :
                        parameters[kv[0]] = frame_args[j]

        obj.parameters = parameters
        obj.ancestors = ancestors
        return obj

    def __init__(self, project, rank = consts.RANKS["CRITICAL"], **kwargs):
        """The first argument must allways be the project. A precess with critical rank will end the run. A process with no critical rank should only end its branch (not implemented, see: recieve_ancestor_join) """
        super(Process, self).__init__()
        
        self.rank = consts.RANKS["CRITICAL"]
        self.status = consts["STATUS"]["PENDING"]
        
        self.project = project

        self.name = self.__class__.__name__
        
        self.ancestors_ready = set()
        self.ancestors_finished = set()
        
        if not hasattr(self, "descendants") :
           self.descendants = []
        self.result = None
        
        self.project.register_process(self)
    
    def update_critical_rank(self, rank) :
        """Update the rank of impotance of the process"""
        self.rank = rank

    def _db_create(self) :
        """create the process in the database"""
        import time
 
        self.arango_doc = self.project.database["Processes"].createDocument()
        self.arango_doc.set(
            {
                "start_date" : time.time(),
                "project": self.project.arango_doc._id,
                "status": self.status,
                "name": self.name,
                "rank": self.rank,
                "parameters" : self.parameters,
                "description" : self.__class__.__doc__
            }
        )
        self.arango_doc.save()

    def register_descendant(self, process) :
        """Register a process as being a descendent of self"""
        self.descendants.append(process)

    def join(self) :
        """joins deceendents at the end of the run"""
        for d in self.descendants :
            d.recieve_ancestor_join(self)
    
    def recieve_ancestor_join(self, process) :
        """receive an end of run notification from an ancestor. If process has at least one of it's ancestors termiate with a error, it will raise a RuntimeError"""
        self.ancestors[process]["status"] = process.status
        self.ancestors_finished.add(self.ancestors[process]["argument_name"])
        
        if process.status == consts["STATUS"]["DONE"] :
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

    def _run(self) :
        """private run function, takes care notifications and updating status"""
        def update_end_date() :
            import time
            self.arango_doc["end_date"] = time.time()
            self.arango_doc.patch()

        try:
            self.result = self.run()
        except Exception as e:
            self.update_status(consts["STATUS"]["ERROR"])
            update_end_date()
            self.project.notify_error(self)
        else :
            self.update_status(consts["STATUS"]["DONE"])
            update_end_date()
            self.join()

    def run(self) :
        """the function that users must redefine, must runs and return the output"""
        raise NotImplementedError("Must be implemented in child")

    def __call__(self) :
        return self.result

class Result(Process):
    """A result is a process with (usually) low critical rank that takes care of fromating results, saving in the database or serializaing them to disk"""
    
    def __init__(self, project, rank = consts.RANKS["NOT_CRITICAL"], **kwargs):
        super(Result, self).__init__(project = project, rank = rank, **kwargs)

    def _db_create(self) :
        """create the process and its result in the db"""
        import time
 
        self.arango_doc = self.project.database["Results"].createDocument()
        self.arango_doc.set(
            {
                "start_date" : time.time(),
                "project": self.project.arango_doc._id,
                "status": self.status,
                "name": self.name,
                "parameters" : self.parameters,
                "rank": self.rank,
                "description" : self.__class__.__doc__
            }
        )
        self.arango_doc.save()
        