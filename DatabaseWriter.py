import pymongo

class DatabaseWriter(object):

    "' Integration with MongoDB to write and retrieve the inverted indexes '"

    def __init__(self):
        pass

    def createDatabase(self, database_name = 'default'):
        pass

    def writeToDatabase(self, index, documents):
        pass

    def retrieveFromDatabase(self):
        pass

    def updateDatabase(self, index, documents):
        pass