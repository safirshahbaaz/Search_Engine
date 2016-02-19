import shelve
from collections import namedtuple
# PageInfo = namedtuple('PageInfo', 'important, normal, links, referral_count')

class Indexer(object):
	
	def __init__(self):
		pass

	def load_shelve(self):
		self.all_info = shelve.open('all_info.shelve', writeback=True)
		'''all_info[url] = (important, normal, links, referral_count)'''
		# print(len(self.all_info))
		# for url in self.all_info:
		# 	print url, self.all_info[url]
		# 	# print self.all_info[url][0]
		# 	break
		self.important_inverted_index = shelve.open('imp_inverted_index.shelve')
		# print len(self.important_inverted_index)
		# for word in self.important_inverted_index:
		# 	print word, " - ",self.important_inverted_index[word]

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
			word_visited = {}	# to track if the word is already considered	
			for word in important_tokens:
					if word not in word_visited:
						tf, loc = self.calc_tf_loc(word, important_tokens)
						# get the current list for the word
						current_list = imp_inv_index.get(word, [])
						# add the new info
						current_list.append({'url':url, 'tf':tf, 'loc':loc})
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


if __name__ == '__main__':
	ind = Indexer()
	ind.load_shelve()
	# ind.update_frwd_index()
	# ind.create_inverted_index()
	# tf, loc = ind.calc_tf_loc('graduation', ['graduation', 'beyond', 'bren', 'school', 'information', 'computer', 'sciences', 'education', 'people', 'community', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation', 'graduation'])
	# print tf, loc

