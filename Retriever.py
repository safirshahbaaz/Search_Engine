import DatabaseWriter as dw
from Processor import Processor

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
		common_keys = set()	# variable to store the common keys in case of multiple query terms

		if index_name == "Important":
			table = self.imp_inv_index_table
		elif index_name == "Normal":
			table = self.nrml_inv_index_table

		for token in query_tokens:
			results = {}
			results_cursor = self.client.retrieveFromInvertedIndexDatabase(table, token)

			for document in results_cursor:
				contents = document['contents']
				for info in contents:
					url = info['url']
					
					# code segment to fetch the referral count
					# frwd_index_cursor = self.client.retrieveFromForwardIndexDatabase(self.frwd_index_table, url)
					# referral_count = frwd_index_cursor.next()['contents'][3]
					
					# print url, " - ",referral_count, " : ",info['tf_idf']

					score = info['tf_idf']
					results[url] = score
				if len(common_keys) == 0:
					common_keys = set(results)
				else:
					common_keys = set(results) & common_keys
			result_sets.append(results)
		
		return result_sets, common_keys




class Ranker(object):

	def __init__(self):
		self.client = dw.DatabaseWriter()
		self.database = self.client.accessDatabase()
		self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)
		self.imp_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndex')
		self.nrml_inv_index_table = self.client.accessInvertedIndexCollection(self.database, 'InvertedIndexNormal')

	def rank(self, result_sets, common_keys):
		# if there are more than 1 result sets then we need to perform joining 
		if len(result_sets) > 1:
			combined_results = {}
			for key in common_keys:
				for result in result_sets:
					combined_results[key] = combined_results.get(key, 0) + result[key]
			results_dict = combined_results

		else:
			results_dict = result_sets[0]
		
		# print(str(len(results_dict)) + " results found")
		# print("----------------------------------------")
		# for result in sorted(results_dict, key=results_dict.get, reverse=True):
		# 	# code segment to fetch the referral count
		# 	frwd_index_cursor = self.client.retrieveFromForwardIndexDatabase(self.frwd_index_table, result)
		# 	referral_count = frwd_index_cursor.next()['contents'][3]
		# 	print result, "-", results_dict[result], "-", referral_count

		results_sorted_dict = {key: results_dict[key] for key in sorted(results_dict, key=results_dict.get, reverse=True)}

		return results_sorted_dict


def runner(query):
	qp = QueryProcessor()
	ar = AnswerRetriever()
	ranker = Ranker()

	query_tokens = qp.process_query(query)
	
	result_sets, common_keys = ar.retrieve_results(query_tokens, "Important")
	final_results = ranker.rank(result_sets, common_keys)


	if len(final_results) < 10:
		# fetch from InvertedIndexNormal
		result_sets, common_keys = ar.retrieve_results(query_tokens, "Normal")
		final_results.update(ranker.rank(result_sets, common_keys))

	for result in sorted(final_results, key=final_results.get, reverse=True):
		print result, "-", final_results[result]

if __name__ == '__main__':
	runner("crista lopes")


