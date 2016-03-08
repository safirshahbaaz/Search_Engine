import DatabaseWriter as dw
from Processor import Processor
from math import log
import config

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
				print result, "-", results_dict[result], "-", page_rank_score
				
				results_dict[result] = results_dict[result] + page_rank_score

		return results_dict


def runner(query):
	qp = QueryProcessor()
	ar = AnswerRetriever()
	ranker = Ranker()
	result_lists =[]	
	
	query_tokens = qp.process_query(query)
	
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
			result_lists.append(result)


	return result_lists

if __name__ == '__main__':
	ans = runner("machine learning")

	print("----------Results-------------")
	for result in ans:
		print(result)
	



