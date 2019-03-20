""""
API sketch

/projects/ #param = status

/projects/{project}/processes/ #param = status
/projects/{project}/processes/{process_key}

/projects/{project}/results/ #param = status
/projects/{project}/results/{result_key}
"""

class ArangoFlowAPI(object):
    """docstring for ArangoFlowAPI"""
    def __init__(self, database):
        super(ArangoFlowAPI, self).__init__()
        self.database = database
    
    def get_projects(self) ;
        pass
    
    def get_process(self, project_name, status) :
        pass

    def get_results(self, project_name, status) :
        pass

    def get_process(self, project_name, process_key) :
        pass

    def get_result(self, project_name, result_key) :
        pass