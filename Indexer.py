import shelve
from math import log10
from collections import namedtuple
import DatabaseWriter as dw

# PageInfo = namedtuple('PageInfo', 'important, normal, links, referral_count')

class Indexer(object):
    def __init__(self):
        self.client = dw.DatabaseWriter()
        self.database = self.client.accessDatabase()
        self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)
        self.imp_inv_index_table = self.client.accessInvertedIndexCollection(self.database)


    def load_collections(self):
        # self.all_info = shelve.open('all_info.shelve', writeback=True)
        self.frwd_index_cursor = self.client.retrieveAllFromForwardIndexDatabase(self.frwd_index_table)
        '''frwd_index_cursor[url] = (important, normal, links, referral_count)'''
        self.total_docs = self.frwd_index_cursor.count()
        print(self.total_docs)
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

    def create_inverted_index(self):
        """
        Function to create inverted index using the frwd index
        It can create 2 indexes (important and normal)
        """
        frwd_index = self.all_info
        imp_inv_index = self.important_inverted_index

        for url in frwd_index:
            important_tokens = frwd_index[url][0]
            word_visited = {}  # to track if the word is already considered
            for word in important_tokens:
                if word not in word_visited:
                    tf, loc = self.calc_tf_loc(word, important_tokens)
                    # get the current dict for the word
                    current_dict = imp_inv_index.get(word, {})
                    # add the new info
                    current_dict[url] = {'tf': tf, 'loc': loc}
                    # add it to the dictionary
                    imp_inv_index[word] = current_dict
                    imp_inv_index.sync()
                    # mark this word as seen for the current list of tokens
                    word_visited[word] = True

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

    def update_inverted_index(self):
        """
        Function to add tf_idf to the inverted index
        """
        imp_inv_index = self.important_inverted_index
        for word in imp_inv_index:
            # load the details dict for the current word
            word_details_dict = imp_inv_index[word]
            # the no. of url's(keys) in the dict will signify the no. of docs it appears
            word_in_docs = len(word_details_dict)
            for url in word_details_dict:
                # load detail for current page
                detail = word_details_dict[url]
                tf = detail['tf']
                tf_idf = self.cal_tf_idf(tf, word_in_docs)
                detail['tf_idf'] = tf_idf
                # update detail
                word_details_dict[url] = detail
                imp_inv_index.sync()

    def cal_tf_idf(self, tf, nk):
        """
        Function to calculate tf_idf
        """
        tf_idf = log10(1 + int(tf)) * log10(self.total_docs / int(nk))
        return tf_idf


if __name__ == '__main__':
    ind = Indexer()
    ind.load_collections()
    ind.update_frwd_index()
    # ind.create_inverted_index()
    # ind.update_inverted_index()
    # tf, loc = ind.calc_tf_loc('graduation', ['graduation', 'beyond', 'bren', 'school', 'information', 'computer', 'sciences', 'education', 'people', 'community', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation'])
    # print tf, loc
