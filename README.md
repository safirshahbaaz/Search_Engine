# Search Engine

## Introduction
This repository contains the complete implementation of a search engine(minus the crawler which can be found at the git repo, https://github.com/Mondego/crawler4py). The search engine, implemented in Python, is capable of returning appropriate search results based on the search query entered. A search engine mainly comprises of 4 parts:

1. A tokenizer: To tokenize the words seen on HTML pages. Verious strategies can be employed here such as stemming and transformations.
2. A web crawler: A crawler starts from a seed URL and finds all the links present in that HTML page and adds those links to a queue for processing later on. At the same time, the body of the HTML page is stored for tokenization later in the process. It is important for the crawler to be able to recognize links that it has already crawled so that it does not get stuck in an infinite loop. 
3. A Database: Once the crawling phase is complete, we have link and set of tokens association that are stored in a database(Mongo DB in our case). It is now needed of use to invert this database i.e. to make the token as the key and the URL as the value for later searching.
4. Web Interface: A simple HTML page with a text box and a submit button to display the results.

## Core concepts used to improve NDCG scores
We have used the concept of hub and authoritative pages to improve our NDCG scores(we used Google as our oracle). A token that appears in the anchor text of an HTML page is given higher priority than tokens that appear in the body of the page. 

## Scope for Improvement
Ideally, the web crawler works in a distributed manner with the queue being shared between several systems. This greatly improves the speed of crawling. The same case applies to creating the inverted index. In fact, MapReduce would be a good paradigm to use in this scenario.
