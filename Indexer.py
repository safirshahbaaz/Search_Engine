import shelve
from math import log10
from collections import namedtuple
import DatabaseWriter as dw
import cProfile

# PageInfo = namedtuple('PageInfo', 'important, normal, links, referral_count')

class Indexer(object):
    def __init__(self):
        self.client = dw.DatabaseWriter()
        self.database = self.client.accessDatabase()
        self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)
        self.imp_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndex')
        self.nrml_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndexNormal')
        self.nrml_bulk_writer = self.nrml_inv_index_table.initialize_ordered_bulk_op()
        self.imp_bulk_writer = self.imp_inv_index_table.initialize_ordered_bulk_op()


    def load_collections(self):
        # self.all_info = shelve.open('all_info.shelve', writeback=True)
        self.frwd_index_cursor = self.client.retrieveAllFromForwardIndexDatabase(self.frwd_index_table)
        '''frwd_index_cursor[url] = (important, normal, links, referral_count)'''
        self.total_docs = self.frwd_index_cursor.count()
        print(self.total_docs)

        # inv_cursor = self.client.retrieveAllFromInvertedIndexDatabase(self.imp_inv_index_table)
        # for doc in inv_cursor:
        #     l = doc['contents']
        #     len_l = len(l)
        #     for d in l:
        #         print d['tf'], d['tf_idf'], len_l
        # i = 0
        # for row in self.frwd_index_cursor:
        # 	print row['url'], row['contents'][3]
        # 	# print self.frwd_index_cursor[url][0]
        # 	if i>2:
        # 		break
        # 	i += 1
        # self.imp_inv_index_table = shelve.open('imp_inverted_index.shelve', writeback=True)
        # print(len(self.imp_inv_index_table))
        # i = 1
        # for word in self.imp_inv_index_table:
        #     print word, " - ", self.imp_inv_index_table[word]
        #     if i > 10:
        #         break
        #     i += 1

    def update_frwd_index(self):
        """
        Function to go over the links on each page and update the important tokens of the referred page
        along with incrementing the referral count (PageRank)
        """
        # frwd_index = self.all_info
        self.frwd_index_cursor = self.client.retrieveAllFromForwardIndexDatabase(self.frwd_index_table)
        '''frwd_index_cursor[url] = (important, normal, links, referral_count)'''
        self.total_docs = self.frwd_index_cursor.count()
        print(self.total_docs)

        for document in self.frwd_index_cursor:
            # url = document['url']
            page_info = document['contents']
            # important = page_info[0]
            links = page_info[2]

            '''
            Go over the links present on this url/page and see which pages it refers
            links: list of (abs_url, link_text_tokens)
            '''
            for link_url, link_text_tokens in links:
                # if the link found on the page is present in the document set
                link_url_info_cursor = self.client.retrieveFromForwardIndexDatabase(self.frwd_index_table, link_url)

                if link_url_info_cursor.count() > 0:
                    # add the link text tokens to the important list and increment referral count by 1
                    # load tuple of the referred url
                    link_url_information_contents = link_url_info_cursor.next()['contents']

                    # store contents of the tuple
                    important_tokens = link_url_information_contents[0]
                    normal_tokens = link_url_information_contents[1]
                    l = link_url_information_contents[2]
                    referral_count = int(link_url_information_contents[3])

                    # adding the link text token to the important tokens list of the page
                    important_tokens.extend(link_text_tokens)
                    # increasing the referral count by 1
                    referral_count += 1

                    # write tuple with updated information
                    # frwd_index[link_url] = important_tokens, normal_tokens, l, referral_count
                    updated_contents = important_tokens, normal_tokens, l, referral_count
                    self.client.updateForwardIndexDatabase(self.frwd_index_table, link_url, updated_contents)
                    # sync back to the file
                    # frwd_index.sync()

    def create_inverted_index(self, collection_type):
        """
        Function to create inverted index using the frwd index
        It can create 2 indexes (important and normal)
        """
        self.frwd_index_cursor = self.client.retrieveAllFromForwardIndexDatabase(self.frwd_index_table)

        i = 0 
        for document in self.frwd_index_cursor:
            i += 1
            if i % 10 == 0:
                print(str(i) + " documents processed in create_inverted_index for " + collection_type)
            url = document['url']
            page_info = document['contents']

            if collection_type == "Important":
                tokens = page_info[0]
                bulk_writer = self.imp_bulk_writer
                table = self.imp_inv_index_table
            elif collection_type == "Normal":
                tokens = page_info[1]
                bulk_writer = self.nrml_bulk_writer
                table = self.nrml_inv_index_table

            word_visited = {}  # to track if the word is already considered
            for word in tokens:

                if word not in word_visited:
                    tf, loc = self.calc_tf_loc(word, tokens)

                    inv_cursor = self.client.retrieveFromInvertedIndexDatabase(table, word)

                    if inv_cursor.count() == 0:
                        # if not found then write
                        current_list = []
                        current_list.append({'url': url, 'tf': tf, 'loc': loc})
                        self.client.writeToInvertedIndexDatabase(table, word, current_list)
                    else:
                        # if found then fetch the old dict and update
                        current_list = inv_cursor.next()['contents']
                        current_list.append({'url': url, 'tf': tf, 'loc': loc})

                        self.client.addToBulkWriter(bulk_writer, word, current_list)
                        #bulk_writer.find({'word': word}).update({'$set': {'contents': current_list}})
                        #self.client.updateInvertedIndexDatabase(table, word, current_list)

                    # mark this word as seen for the current list of tokens
                    word_visited[word] = True

            # write at every 100 documents
            if i % 100 == 0:
                self.client.updateBulkContentToDatabase(bulk_writer)
                if collection_type == "Important":
                    self.imp_bulk_writer = self.imp_inv_index_table.initialize_ordered_bulk_op()
                    bulk_writer = self.imp_bulk_writer
                elif collection_type == "Normal":
                    self.nrml_bulk_writer = self.nrml_inv_index_table.initialize_ordered_bulk_op()
                    bulk_writer = self.nrml_bulk_writer
        
        # write the remaining docs
        self.client.updateBulkContentToDatabase(bulk_writer)


    def calc_tf_loc(self, word, tokens):
        """
        Function to calculate the tf and the location of the terms in a list of tokens
        """
        tf = 0
        loc = []
        for i in xrange(len(tokens)):
            if word == tokens[i]:
                tf += 1
                loc.append(i)
        return tf, loc

    def update_inverted_index(self, collection_type):
        """
        Function to add tf_idf to the inverted index
        """
        # quick fix for total docs count
        self.frwd_index_cursor = self.client.retrieveAllFromForwardIndexDatabase(self.frwd_index_table)
        self.total_docs = self.frwd_index_cursor.count()

        if collection_type == "Important":
            self.imp_bulk_writer = self.imp_inv_index_table.initialize_ordered_bulk_op()
            bulk_writer = self.imp_bulk_writer
            table = self.imp_inv_index_table
        elif collection_type == "Normal":
            self.nrml_bulk_writer = self.nrml_inv_index_table.initialize_ordered_bulk_op()
            bulk_writer = self.nrml_bulk_writer
            table = self.nrml_inv_index_table
        
        inv_cursor = self.client.retrieveAllFromInvertedIndexDatabase(table)

        i = 0
        for document in inv_cursor:
            i += 1
            if i % 10 == 0:
                print(str(i) + " documents processed in update_inverted_index for " + collection_type)
            word = document['word']

            # load the details list for the current word
            word_details_list = document['contents']
            # the no. of dicts in the list will signify the no. of docs it appears
            word_in_docs = len(word_details_list)
            
            for detail_dict in word_details_list:
                # load detail for current page
                tf = detail_dict['tf']

                tf_idf = self.cal_tf_idf(tf, word_in_docs)

                detail_dict['tf_idf'] = tf_idf
            
            # self.client.updateInvertedIndexDatabase(table, word, word_details_list)
            self.client.addToBulkWriter(bulk_writer, word, word_details_list)
            
            # write at every 100 documents
            if i % 100 == 0:
                self.client.updateBulkContentToDatabase(bulk_writer)

                if collection_type == "Important":
                    self.imp_bulk_writer = self.imp_inv_index_table.initialize_ordered_bulk_op()
                    bulk_writer = self.imp_bulk_writer
                elif collection_type == "Normal":
                    self.nrml_bulk_writer = self.nrml_inv_index_table.initialize_ordered_bulk_op()
                    bulk_writer = self.nrml_bulk_writer
        
        # write the remaining docs
        self.client.updateBulkContentToDatabase(bulk_writer)

    def cal_tf_idf(self, tf, nk):
        """
        Function to calculate tf_idf
        """
        tf_idf = float(1 + log10(int(tf))) * float(log10(self.total_docs / float(nk)))
        return tf_idf


if __name__ == '__main__':
    ind = Indexer()
    ind.load_collections()
    ind.update_frwd_index()
    ind.create_inverted_index("Important")
    ind.update_inverted_index('Important')
    cProfile.run("ind.create_inverted_index('Normal')")
    ind.update_inverted_index('Normal')
    # tf, loc = ind.calc_tf_loc('graduation', ['graduation', 'beyond', 'bren', 'school', 'information', 'computer', 'sciences', 'education', 'people', 'community', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation'])
    # print tf, loc
