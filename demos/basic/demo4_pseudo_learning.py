
from ArangoFlow import template as template
import numpy

class PseudoDataset(template.Process):
    """"""
    def __init__(self, project, size):
        super(PseudoDataset, self).__init__(project)
        self.size = size

    def run(self) :
        return {
            "train": numpy.random.random(self.size),
            "test": numpy.random.random(self.size),
            "validation": numpy.random.random(self.size),
        }

class PseudoLearner(template.Process):
    """"""
    def __init__(self, project, train, test, validation):
        super(PseudoLearner, self).__init__(project)
        self.train = train
        self.test = test
        self.validation = validation

    def run(self) :
        import random
        for i in range(10) :
            acc = {
                    "train": random.random() * 100,
                    "test": random.random() * 100,
                    "validation": random.random() * 100,
                }
            self.tick("accuracy", acc)

        return acc

class PseudoCurve(template.Monitor) :
    """"""
    def __init__(self, project, tick_input, **kwargs):
        super(PseudoCurve, self).__init__(project, tick_input, **kwargs)
        self.data = data
        
    def tick_run(self, process, value) :
        self.data.append(value)

    def run(self) :
        print(self.data)
        return self.data

class PseudoSelector(template.Process):
    """"""
    def __init__(self, project, model_inputs):
        super(PseudoSelector, self).__init__(project)
        self.model_inputs = model_inputs

    def run(self) :
        best, ind = None, 0
        for i, v in enumerate(self.model_inputs) :
            if not best :
                best, ind = v, i
            elif v()["validation"] < best()["validation"] :
                best, ind = v, i

        # self.arango_doc["best_model_process_id"] = v.arango_doc["_id"] 
        # self.arango_doc["best_model_process_uuid"] = v.arango_doc["_uuid"] 
        # self.arango_doc["best_model_process_path_uuid"] = v.arango_doc["_path_uuid"] 
        # self.arango_doc["best_model_accuracy"] = best()["validation"]  
        # self.arango_doc.patch()

        print(best())
        return best()

if __name__ == '__main__':
    DB_URL = "http://localhost:8529"
    DB_USERNAME = "root"
    DB_PASSWORD = "root"
    DB_NAME = "ArangoFlow"

    import pyArango.connection as ADB

    connection = ADB.Connection(arangoURL = DB_URL, username = DB_USERNAME, password = DB_PASSWORD)
    try:
        db = connection.createDatabase(DB_NAME)
    except Exception as e:
        db = connection[DB_NAME]
    
    with template.FlowProject(db, "test") as project :
        data = PseudoDataset(project, size = 10)
        learners = []
        tick_handles = []
        for i in range(2) :
            model = PseudoLearner(project, data["train"], data["test"], data["validation"])
            learners.append(model)
            tick_handles.append(model.ticks("accuracy"))

        curves = PseudoCurve(project, tick_handles)
        sel = PseudoSelector(project, learners)
        project.run()
