from pymongo import *

class DatabaseWriter(object):

    "' Integration with MongoDB to write and retrieve the inverted indexes '"

    def __init__(self):
        self.client = MongoClient()

    def accessDatabase(self, database_name = 'default'):
        return self.client['InvertedIndex']

    def accessCollection(self, db):
        return db['Indexes']

    def writeToDatabase(self, collection, index, documents):
        collection.insert_one({'index': index, 'documents': documents})

    def retrieveFromDatabase(self, collection, query):
        return collection.find({"index": query})

    def deleteFromDatabase(self, collection, index):
        collection.delete_many({"index": index})

    def updateDatabase(self, collection, index, documents):
        collection.update_one({"index": index}, {"$set": {"documents": documents}})

if __name__ == '__main__':
    client = DatabaseWriter()
    database = client.accessDatabase()
    collections = client.accessCollection(database)
    client.writeToDatabase(collections, 'Safir', 'There is no fate but what we make')
    client.writeToDatabase(collections, 'xyz', 'Nothing is true')
    client.writeToDatabase(collections, 'pqr', 'Everything is permitted')
    client.writeToDatabase(collections, 'jkl', 'What have you done?')
    for elements in client.retrieveFromDatabase(collections, 'xyz'):
        print elements['documents']

    client.updateDatabase(collections, "xyz", "I just changed you")

    for elements in client.retrieveFromDatabase(collections, 'xyz'):
        print elements['documents']

    client.deleteFromDatabase(collections, 'xyz')

    for elements in client.retrieveFromDatabase(collections, 'xyz'):
        print elements['documents']



    for elements in client.retrieveFromDatabase(collections, 'xyz'):
        print elements['documents']