import shelve
from collections import namedtuple
# PageInfo = namedtuple('PageInfo', 'important, normal, links, referral_count')

class Indexer(object):
	
	def __init__(self):
		pass

	def load_shelve(self):
		self.all_info = shelve.open('all_info.shelve', writeback=True)
		'''all_info[url] = (important, normal, links, referral_count)'''
		print(len(self.all_info))
		for url in self.all_info:
			print url, self.all_info[url]
			# print self.all_info[url][0]
			break

	def create_frwd_index(self):
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
					# load details of the referred url and update the tuple
					link_url_information = frwd_index[link_url]
					
					important_tokens = link_url_information[0]
					normal_tokens = link_url_information[1]
					l = link_url_information[2]
					referral_count = int(link_url_information[3])

					important_tokens.extend(link_text_tokens)
					referral_count += 1
					
					# updating info
					frwd_index[link_url] = important_tokens, normal_tokens, l, referral_count
					# link_url_information = link_url_information._replace(important=important_tokens, referral_count=referral_count)


if __name__ == '__main__':
	ind = Indexer()
	ind.load_shelve()
	# ind.create_frwd_index()
