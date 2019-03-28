
from ArangoFlow import template as template
import numpy

class RandomMatrix(template.Process):
    """Create a random matrix. size is the shape of the matrix"""
    def __init__(self, project, size):
        super(RandomMatrix, self).__init__(project)
        self.size = size

    def run(self) :
        return numpy.random.random(self.size)

class Threshold(template.Process):
    """Only keep values > threshold"""
    def __init__(self, project, previous, threshold):
        super(Threshold, self).__init__(project)
        self.previous = previous
        self.threshold = threshold

    def run(self) :
        mat = self.previous.result
        index = mat > self.threshold
        return mat[index]

class Scale(template.Process):
    """multply the value of the previous element by value"""
    def __init__(self, project, previous, scale):
        super(Scale, self).__init__(project)
        self.previous = previous
        self.scale = scale

    def run(self) :
        # print(self.descendants)
        return self.previous.result * self.scale

class Normalize(template.Process):
    """Substract the mean and divide by the std"""
    def __init__(self, project, previous):
        super(Normalize, self).__init__(project)
        self.previous = previous
        
    def run(self) :
        # print(self.descendants)
        mat = self.previous.result
        avg = numpy.mean(mat)
        std = numpy.std(mat)
        return (mat- avg)/std

class Append(template.Process):
    """Append a vector to vector"""
    def __init__(self, project, mat1, mat2, axis):
        super(Append, self).__init__(project)
        self.mat1 = mat1
        self.mat2 = mat2
        self.axis = axis

    def run(self) :
        return numpy.append( self.mat1(), self.mat2() )

class SerializeMatrix(template.Result):
    """Save a matrix on disk in a txt format"""
    def __init__(self, project, previous, filename):
        super(SerializeMatrix, self).__init__(project)
        self.previous = previous
        self.filename = filename

    def run(self) :
        numpy.savetxt(self.filename, self.previous.result)
        return True

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
        mat = RandomMatrix(project, size = (10, 10))
        tmat = Threshold(project, previous = mat, threshold=0.5)
        mat2 = Normalize(project, previous = tmat)
        
        con = Append(project, tmat, mat2, axis = 1)
        
        project.run()