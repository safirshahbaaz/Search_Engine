from pymongo import *
import config

_SHOW_DEBUG_TRACE = True

class DatabaseWriter(object):

    "' Integration with MongoDB to write and retrieve the inverted indexes '"

    def __init__(self):
        self.client = MongoClient()

    def accessDatabase(self, database_name = 'default'):
        return self.client[config.db_name]

    ''' Forward Index Database Manipulation functions '''

    def accessForwardIndexCollection(self, db):
        return db['ForwardIndex']

    def writeToForwardIndexDatabase(self, collection, url, contents):
        collection.insert_one({'url': url, 'contents': contents})

    def writeAllToForwardIndexDatabase(self, collection, docs_array):
        collection.insert_many(docs_array)

    def retrieveFromForwardIndexDatabase(self, collection, url):
        return collection.find({'url': url})

    def retrieveAllFromForwardIndexDatabase(self, collection):
        return collection.find(no_cursor_timeout=True)    

    def deleteFromForwardIndexDatabase(self, collection, url):
        collection.delete_many({'url': url})

    def updateForwardIndexDatabase(self, collection, url, contents):
        collection.update_one({'url': url}, {"$set": {"contents": contents}})

    def checkIfDocumentExists(self, collection, key, value):
        return collection.find({key: value}).count() > 0

    ''' ------------------------------------------------------------------ '''

    ''' Word Frequency Database manipulation functions '''

    def accessInvertedIndexCollection(self, db, collection_name):
        return db[collection_name]

    def writeToInvertedIndexDatabase(self, collection, word, contents):
        collection.insert_one({'word': word, 'contents': contents})

    def writeAllToInvertedIndexDatabase(self, collection, docs_array):
        collection.insert_many(docs_array)

    def retrieveFromInvertedIndexDatabase(self, collection, word):
        return collection.find({'word': word})

    def retrieveAllFromInvertedIndexDatabase(self, collection):
        return collection.find(no_cursor_timeout=True)  

    def deleteFromInvertedIndexDatabase(self, collection, word):
        collection.delete_many({'word': word})

    def addToBulkWriter(self, bulk_writer, word, contents, tf_idf_update = False):

        if tf_idf_update == False:
            bulk_writer.find({'word': word}).upsert().update({'$addToSet': {'contents': contents}})
        else:
            bulk_writer.find({'word': word}).replace_one({'word': word, 'contents': contents})

    def updateBulkContentToDatabase(self, bulk_writer):
        try:
            result = bulk_writer.execute()

            if _SHOW_DEBUG_TRACE:
                # print result
                print("Writing records to DB")
        except Exception as e:
            print type(e).__name__ + "Error occurred while writing to the database"

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