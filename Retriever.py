import DatabaseWriter as dw
from Processor import Processor
from math import log
import requests
import json
import config
import re
import math

class QueryProcessor(object):

    def __init__(self):
        pass

    def process_query(self, query):
    	query_tokens = Processor().tokenizer(query)
    	return query_tokens

class AnswerRetriever(object):

	def __init__(self):
		self.client = dw.DatabaseWriter()
		self.database = self.client.accessDatabase()
		self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)
		self.imp_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndex')
		self.nrml_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndexNormal')

	def retrieve_results(self, query_tokens, index_name):
		result_sets = []	# variable to store the results retrived from individual query terms

		if index_name == "Important":
			table = self.imp_inv_index_table
		elif index_name == "Normal":
			table = self.nrml_inv_index_table

		for token in query_tokens:
			results_dict = {}
			results_cursor = self.client.retrieveFromInvertedIndexDatabase(table, token)

			for document in results_cursor:
				contents = document['contents']
				for info in contents:
					url = info['url']
					
					# if tf_idf was not updated properly then skip it 
					try:
						results_dict[url] = info['tf_idf']
					except:
						print("Skipping url as no tf_idf is found")
						continue

			result_sets.append(results_dict)
		
		return result_sets




class Ranker(object):

	def __init__(self):
		self.client = dw.DatabaseWriter()
		self.database = self.client.accessDatabase()
		self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)
		self.imp_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndex')
		self.nrml_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndexNormal')

	def rank(self, result_sets):
		# if there are more than 1 result sets then we need to perform joining 
		if len(result_sets) > 1:
			# find the common urls in the result sets
			common_keys = (set.intersection(*(set(d.iterkeys()) for d in result_sets)))

			combined_results = {}
			for key in common_keys:
				for result in result_sets:
					combined_results[key] = combined_results.get(key, 0) + result[key]
			results_dict = combined_results

		else:
			results_dict = result_sets[0]
		
		if config.include_page_rank_score is True:
			for result in results_dict:
				# code segment to fetch the referral count
				frwd_index_cursor = self.client.retrieveFromForwardIndexDatabase(self.frwd_index_table, result)
				referral_count = frwd_index_cursor.next()['contents'][3]
				page_rank_score = config.referral_factor * log(referral_count+2, 2)
				# print result, "-", results_dict[result], "-", referral_count
				
				results_dict[result] = results_dict[result] + page_rank_score

		return results_dict

class NDCG(object):

	def __init__(self):
		pass

	def retrieve_google_results(self, query):
		query = query.replace(" ", "+")
		req = "https://www.googleapis.com/customsearch/v1?key=" + config.key + "&cx=" + config.cx + "&q=" +query+"&filter=1&start=1&num=10&alt=json"
		r = requests.get(req)
		content_dict = json.loads(r.content)
		results_1 = [content_dict['items'][i]['link'].rstrip("/").replace("https://", "http://") for i in xrange(10)]

		req = "https://www.googleapis.com/customsearch/v1?key=" + config.key + "&cx=" + config.cx + "&q="+query+"&filter=1&start=11&num=10&alt=json"
		r = requests.get(req)
		content_dict = json.loads(r.content)
		results_2 = [content_dict['items'][i]['link'].rstrip("/").replace("https://", "http://") for i in xrange(10)]

		results_1.extend(results_2)
		final_results = []
		for result in results_1:
			if not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
                + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                + "|thmx|mso|arff|rtf|jar|csv" \
                + "|rm|smil|wmv|swf|wma|zip|rar|gz|rss)$", result):
				final_results.append(result)

		# for result in final_results:
		# 	print result

		return final_results

	def calc_ndcg(self, google_result, my_result, n):
		google_score = []
		my_score = []
		google_cumul = []
		my_cumul = []
		for i in range(n):
			google_score.append(n-i)    # google scores 5 4 3 2 1 or adjust like 5 4.5 4....
			my_score.append(0)          # our scores initialized
			google_cumul.append(0)      # google cumulative
			my_cumul.append(0)          # our cumulative
		for i in range(n):
			if google_result[i] in my_result:
				my_score[my_result.index(google_result[i])]= google_score[i]  #if match found assign corresponding score
		for i in range(n):
			if i == 0:
				google_cumul[i] = google_score[i]     # assign cumulative scores google
				my_cumul[i] = my_score[i]             # assign our cumul scores
			else:
				google_cumul[i] = google_cumul[i-1]+(google_score[i]/math.log(i+1,2))
				my_cumul[i] = my_cumul[i-1]+(my_score[i]/math.log(i+1,2))
		print "GoogleScore:     ",google_score
		print "MyScore:         ",my_score
		print "GoogleCumulative:",google_cumul
		print "MyCumulative:    ",my_cumul
		match = 0
		Ndcg = []
		for i in range(n):
			###if abs(my_cumul[i]-google_cumul[i])<=1: match+=1   #number of near matches based on cumulative scores
			Ndcg.append( my_cumul[i]/google_cumul[i])  # my cumulative/ google cumulative at each step
		print "Ndcg:            ",Ndcg
		print "Final quality:   ",Ndcg[-1] # closer to 1 is best

	# def calc_ndcg(self, google_result, my_result, ng, nm):
	# 	google_score = []
	# 	my_score = []
	# 	google_cumul = []
	# 	my_cumul = []
	#
	# 	for i in range(ng):
	# 		google_score.append(ng-i)    # google scores 5 4 3 2 1 or adjust like 5 4.5 4....
	# 		# google_cumul.append(0)      # google cumulative
	#
	#
	# 	for j in range(nm):
	# 		my_score.append(0)          # our scores initialized
	# 		my_cumul.append(0)          # our cumulative
	#
	# 	for i in range(ng):
	# 		if google_result[i] in my_result:
	# 			my_score[my_result.index(google_result[i])] = google_score[i]  #if match found assign corresponding score
	#
	# 	for i in range(nm):
	# 		if i == 0:
	# 			# google_cumul[i] = google_score[i]     # assign cumulative scores google
	# 			my_cumul[i] = my_score[i]             # assign our cumul scores
	# 		else:
	# 			# google_cumul[i] = google_cumul[i-1]+(google_score[i]/math.log(i+1,2))
	# 			my_cumul[i] = my_cumul[i-1]+(my_score[i]/math.log(i+1, 2))
	#
	# 	# calculate max possible google score
	# 	google_max_score = ng
	# 	for i in range(1, nm):
	# 		google_max_score += (ng-i)/math.log(i+1,2)
	#
	# 	print "GoogleScore:     ",google_score
	# 	print "MyScore:         ",my_score
	# 	print "MyCumulative:    ",my_cumul
	# 	print "Final quality:   ",my_cumul[-1]/google_max_score # closer to 1 is best




def runner(query):
	qp = QueryProcessor()
	ar = AnswerRetriever()
	ranker = Ranker()
	result_lists =[]	
	
	query_tokens = qp.process_query(query)

	'''Old Logic
	
	result_sets = ar.retrieve_results(query_tokens, "Important")
	important_results = ranker.rank(result_sets)

	# sort and add the important results separately
	for result in sorted(important_results, key=important_results.get, reverse=True):
		# print type(result)
		result_lists.append(result)

	if len(result_lists) < 10:
		# fetch from InvertedIndexNormal
		result_sets= ar.retrieve_results(query_tokens, "Normal")
		normal_results = ranker.rank(result_sets)

		for result in sorted(normal_results, key=normal_results.get, reverse=True):
			# print result, "-", normal_results[result]
			if result not in result_lists:
				result_lists.append(result)

	'''

	# With combination
	result_sets = ar.retrieve_results(query_tokens, "Important")
	important_results = ranker.rank(result_sets)



	if len(result_lists) < 10:
		# fetch from InvertedIndexNormal
		result_sets= ar.retrieve_results(query_tokens, "Normal")
		normal_results = ranker.rank(result_sets)

		# sort and add the important results separately
		for result in important_results:
			if result in normal_results:
				# add the score from the normal results
				important_results[result] = important_results.get(result) + normal_results.get(result)
				# remove result from the normal results
				normal_results.pop(result)

		# add remaining results to important results
		important_results.update(normal_results)

	for result in sorted(important_results, key=important_results.get, reverse=True):
		# print result, "-", important_results[result]
		result_lists.append(result)



	return result_lists

if __name__ == '__main__':
	ndcg = NDCG()
	queries = ['mondego', 'machine learning', 'software engineering', 'security', 'student affairs', 'graduate courses', 'crista lopes', 'rest', 'computer games', 'information retrieval']
	# queries = ["information retrieval"]

	for query in queries:
		print "-----Query: ", query, "-----"
		ans = runner(query)

		# print("----------Our Results-------------")
		# for result in ans:
		# 	print(result)


		# print("----------Google Results-------------")
		g_results = ndcg.retrieve_google_results(query)

		for i in range(10 - len(g_results)):
			g_results.append("")

		ndcg.calc_ndcg(g_results[0:5], ans[0:5], 5)

	# ndcg.calc_ndcg([1 ,2, 3, 4, 5 ],[8, 7 ,31, 2 ,90],5) #google list, our list, list length
	# ndcg.calc_ndcg([1 ,2, 3, 4, 5 ,6,7, 8, 9, 10],[1, 2, 3 ,4, 0],10, 5) #google list, our list, list length


	



