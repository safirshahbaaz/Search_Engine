import shelve
from math import log10
from collections import namedtuple


# PageInfo = namedtuple('PageInfo', 'important, normal, links, referral_count')

class Indexer(object):
    def __init__(self):
        pass

    def load_shelve(self):
        self.all_info = shelve.open('all_info.shelve', writeback=True)
        '''all_info[url] = (important, normal, links, referral_count)'''
        self.total_docs = len(self.all_info)
        print(self.total_docs)
        # i = 0
        # for url in self.all_info:
        # 	print url, self.all_info[url]
        # 	# print self.all_info[url][0]
        # 	if i>2:
        # 		break
        # 	i += 1
        self.important_inverted_index = shelve.open('imp_inverted_index.shelve', writeback=True)
        print(len(self.important_inverted_index))
        # i = 1
        # for word in self.important_inverted_index:
        #     print word, " - ", self.important_inverted_index[word]
        #     if i > 2:
        #         break
        #     i += 1

    def update_frwd_index(self):
        """
        Function to go over the links on each page and update the important tokens of the referred page
        along with incrementing the referral count (PageRank)
        """
        frwd_index = self.all_info

        for url in frwd_index:
            page_info = frwd_index[url]
            # important = page_info[0]
            links = page_info[2]

            '''
            Go over the links present on this url/page and see which pages it refers
            links: list of (abs_url, link_text_tokens)
            '''
            for link_url, link_text_tokens in links:
                # if the link found on the page is present in the document set
                if link_url in frwd_index:
                    # add the link text tokens to the important list and increment referral count by 1
                    # load tuple of the referred url
                    link_url_information = frwd_index[link_url]

                    # store contents of the tuple
                    important_tokens = link_url_information[0]
                    normal_tokens = link_url_information[1]
                    l = link_url_information[2]
                    referral_count = int(link_url_information[3])

                    # adding the link text token to the important tokens list of the page
                    important_tokens.extend(link_text_tokens)
                    # increasing the referral count by 1
                    referral_count += 1

                    # write tuple with updated information
                    frwd_index[link_url] = important_tokens, normal_tokens, l, referral_count
                    # sync back to the file
                    frwd_index.sync()

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
                    # get the current list for the word
                    current_list = imp_inv_index.get(word, [])
                    # add the new info
                    current_list.append({'url': url, 'tf': tf, 'loc': loc})
                    # add it to the dictionary
                    imp_inv_index[word] = current_list
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
            word_details_list = imp_inv_index[word]
            word_in_docs = len(word_details_list)
            for i in xrange(len(word_details_list)):
                # load detail for current page
                detail = word_details_list[i]
                tf = detail['tf']
                tf_idf = self.cal_tf_idf(tf, word_in_docs)
                detail['tf_idf'] = tf_idf
                # update detail
                word_details_list[i] = detail
                imp_inv_index.sync()

    def cal_tf_idf(self, tf, nk):
        """
        Function to calculate tf_idf
        """
        tf_idf = log10(1 + int(tf)) * log10(self.total_docs / int(nk))
        return tf_idf


if __name__ == '__main__':
    ind = Indexer()
    ind.load_shelve()
    # ind.update_frwd_index()
    # ind.create_inverted_index()
    # ind.update_inverted_index()
    # tf, loc = ind.calc_tf_loc('graduation', ['graduation', 'beyond', 'bren', 'school', 'information', 'computer', 'sciences', 'education', 'people', 'community', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation'])
    # print tf, loc
