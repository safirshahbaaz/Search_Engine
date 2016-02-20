from pymongo import *

class DatabaseWriter(object):

    "' Integration with MongoDB to write and retrieve the inverted indexes '"

    def __init__(self):
        self.client = MongoClient()

    def accessDatabase(self, database_name = 'default'):
        return self.client['Index']

    ''' Forward Index Database Manipulation functions '''

    def accessForwardIndexCollection(self, db):
        return db['ForwardIndex']

    def writeToForwardIndexDatabase(self, collection, url, contents):
        collection.insert_one({'url': url, 'contents': contents})

    def retrieveFromForwardIndexDatabase(self, collection, url):
        return collection.find({'url': url})

    def retrieveAllFromForwardIndexDatabase(self, collection):
        return collection.find()    

    def deleteFromForwardIndexDatabase(self, collection, url):
        collection.delete_many({'url': url})

    def updateForwardIndexDatabase(self, collection, url, contents):
        collection.update_one({'url': url}, {"$set": {"contents": contents}})

    def checkIfDocumentExists(self, collection, key, value):
        return collection.find({key: value}).count() > 0

    ''' ------------------------------------------------------------------ '''

    ''' Word Frequency Database manipulation functions '''

    def accessInvertedIndexCollection(self, db):
        return db['InvertedIndex']

    def writeToInvertedIndexDatabase(self, collection, word, contents):
        collection.insert_one({'word': word, 'contents': contents})

    def retrieveFromInvertedIndexDatabase(self, collection, word):
        return collection.find({'word': word})

    def retrieveAllFromInvertedIndexDatabase(self, collection):
        return collection.find()  

    def deleteFromInvertedIndexDatabase(self, collection, word):
        collection.delete_many({'word': word})

    def updateInvertedIndexDatabase(self, collection, word, contents):
        collection.update_one({'word': word}, {"$set": {"contents": contents}})

if __name__ == '__main__':
    client = DatabaseWriter()
    database = client.accessDatabase()

    forward_index_collection = client.accessForwardIndexCollection(database)

    client.writeToForwardIndexDatabase(forward_index_collection, 'http://www.ics.uci.edu/~yuxiaow1',
                                       (['yuxiao', 'website'], ['current', 'position', 'phd', 'student', 'department', 'statistics', 'university', 'california', 'irvine', 'research', 'interest', 'spatio', 'temporal', 'data', 'modeling', 'time', 'series', 'contact', 'yuxiaow1', 'uci', 'edu', 'phd', 'progress', 'department', 'statistics', 'uc', 'irvine', 'sept', '2012', 'present', 'b', 'physics', '2010', 'university', 'science', 'technology', 'china', 'ustc', 'cortical', 'source', 'reconstruction', 'brain', 'connectivity', 'study', 'uisng', 'eeg', 'data'], [], 1))

    for elements in client.retrieveFromForwardIndexDatabase(forward_index_collection, 'http://www.ics.uci.edu/~yuxiaow1'):
        print elements['contents']