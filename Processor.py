import shelve
import urlparse
import glob
import os.path
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk import word_tokenize
import re
import string
import unicodedata
from collections import namedtuple
import DatabaseWriter as dw

stop_words = stopwords.words("english")

filepath = "./tempfiles/"
# filepath = "./backup/run2/"

class Processor(object):
    """
    Class having utilities to process document data from output txt files
    """

    def __init__(self):
        self.client = dw.DatabaseWriter()
        self.database = self.client.accessDatabase()
        self.frwd_index_table = self.client.accessForwardIndexCollection(self.database)

    def process_file(self):
        """
        Function to extract url, content from the txt files
        :return:
        """
        filelist = glob.glob(filepath + "*")
        for filename in filelist:
            if os.path.isfile(filename):
                print "-------------------------\nFile Found: " + filename + "\n"
                content = ""
                content_type = ""
                first_url = True
                with open(filename, "r") as f:
                    for line in f:
                        if line.startswith("-----=="):

                            if first_url is False:
                                important_tokens, normal_tokens, links = self.process_content(url, content, content_type)
                                self.store_content(url, important_tokens, normal_tokens, links)
                            try:
                                url = line.split("--")[6]
                            except Exception as e:
                                print("could not extract page")

                            content = ""
                            first_url = False

                        elif line.startswith("HTML CONTENT"):
                            content_type = "HTML"
                        elif line.startswith("TEXT CONTENT"):
                            content_type = "TEXT"
                        else:
                            content += line
                    # to handle the last page
                    important_tokens, normal_tokens, links = self.process_content(url, content, content_type)
                    self.store_content(url, important_tokens, normal_tokens, links)

    def process_content(self, url, content, content_type):
        """
        Based on the content type, it processes the content
        :param url: url of the page
        :param content: content of page
        :param content_type: HTML/TEXT
        :return:
        """
        # initializing variables
        important_tokens = []
        normal_tokens = []
        links = []

        if content_type == "HTML":
            # pass
            # print "found html for -", url
            important_tokens, normal_tokens, links = self.process_html(url, content)
            # print important_tokens
            # print normal_tokens
            # print links
        elif content_type == "TEXT":
            # pass
            # print "found text for -", url
            important_tokens, normal_tokens = self.process_text(content)
            # print important_tokens
            # print normal_tokens
        else:
            print content_type

        return important_tokens, normal_tokens, links

    def process_html(self,url, content):
        """
        Function to process html
        :param content: content to process
        :param url: The url is needed to make links absolute
        :return: important_tokens, normal_tokens
        """
        decoded_content = content.decode('U7', 'ignore')  # decoding under U7
        data = unicodedata.normalize('NFKD', decoded_content).encode('ascii', 'ignore')  # normalizing unicode to remove \x chars
        soup = BeautifulSoup(data, 'html.parser')

        important_tokens = []  # list for storing
        normal_tokens = []
        links = []

        try:
            title = str(soup.head.title.string)
            # to handle cases if the tile is blank
            if "none" in title.lower():
                title = ""
            # print self.tokenizer(title)
            important_tokens.extend(self.tokenizer(title))
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract title")
            pass

        try:
            h1 = [str(h.string) for h in soup.find_all('h1') if h.string is not None]
            # print self.tokenizer(' '.join(h1))
            important_tokens.extend(self.tokenizer(h1))
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract h1")
            pass

        try:
            h2 = [str(h.string) for h in soup.find_all('h2') if h.string is not None]
            # print self.tokenizer(' '.join(h2))
            important_tokens.extend(self.tokenizer(h2))
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract h2")
            pass

        try:
            h3 = [str(h.string) for h in soup.find_all('h3') if h.string is not None]
            # print self.tokenizer(' '.join(h3))
            important_tokens.extend(self.tokenizer(h3))
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract h3")
            pass

        try:
            # a = [(urlparse.urljoin(url, a['href']), a.get_text()) for a in soup.find_all('a') if a.has_attr('href')]
            abs_link = ""
            link_text = ""
            # link_title = ""
            link_info = ()
            for a in soup.find_all('a'):
                if a.has_attr('href'):
                    abs_link = urlparse.urljoin(url, a['href']).encode('utf-8', 'ignore')
                    link_text = a.get_text().encode('utf-8', 'ignore')
                    # '''Some links only have a title'''
                    # if a.has_attr('title'): 
                    #     link_title = a.get('title').encode('utf-8', 'ignore')
                    link_text_tokens = self.tokenizer(link_text)
                    # print self.tokenizer(link_title)
                    ''' Storing in a tuple (url, [tokenized list of url text])'''
                    link_info = (abs_link, link_text_tokens)
                    links.append(link_info)
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract a")
            pass

        try:
            for element in soup.find_all(["script", "style", "title", "h1", "h2", "h3"]):
                element.extract()
            body_text = soup.get_text()
            body_text = unicodedata.normalize('NFKD', body_text).encode('ascii', 'ignore')
            # print(''.join(body_text))
            normal_tokens.extend(self.tokenizer(body_text))
        except Exception as e:
            # print(type(e).__name__ + ": Could not extract body text")
            pass

        return important_tokens, normal_tokens, links

    def process_text(self, content):
        """
        Function to extract the title from the text and store important tokens and the normal tokens
        :param content:
        :return:
        """
        decoded_content = content.decode('U7', 'ignore')
        data = unicodedata.normalize('NFKD', decoded_content).encode('ascii', 'ignore')

        important_tokens = []
        normal_tokens = []

        content_lines = data.split('\n')
        i = 1  # this variable will determine in which line we extracted the title
        for line in content_lines:
            '''To extract the title from the text we are extracting the first non-empty line'''
            if line.strip() != "":
                if len(line.strip().split()) < 30:
                    title = line
                    important_tokens.extend(self.tokenizer(title))
                # print(important_tokens)
                break
            i += 1
        normal_tokens.extend(self.tokenizer(' '.join(content_lines[i:])))  # we will consider the lines after the title extraction, just a way to remove title
        return important_tokens, normal_tokens

    def store_content(self, url, important, normal, links):
        """
        Function to store the contents in a shelve file, used for further processing
        """
        referral_count = 0  # initilizing the referral count which is currently not calculated
        # all_info[url] = important, normal, links, referral_count
        # all_info.sync()
        info_tuple = important, normal, links, referral_count
        self.client.writeToForwardIndexDatabase(self.frwd_index_table, url, info_tuple)

    def clean_tokens(self, tklist):
        """

        :param tklist:
        :return:
        """
        tklist = [tk for tk in tklist if tk.isalnum() and "\\x" not in tk]
        return tklist

    def tokenizer(self, content):
        """
        Function to read a input file, tokenize it and return the token list
        :param file_name:
        :return:
        """

        # make translation table to insert spaces in place of below chars
        table = string.maketrans("-.[](){}", "        ")

        # Not removing ' from the input string
        # Delete all chars from s that are in deletechars(if present),and then translate the characters using table
        content = content.translate(table,
                                    '!"#$%&*+,/:;<=>?@\\^_`|~')  # all punctuations = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

        # convert the content to lower case to make it case insensitive
        content = content.lower()

        token_list = []
        for word in content.split():
            if word not in stop_words:
                # if the word ends with 's then remove it
                if word.endswith("'s"):
                    word = word[:-2]    
                token_list.append(word)


        # storing the token count in a class variable
        # self.token_count = len(token_list)
        # print "Total no. of tokens = ", self.token_count
        return token_list


if __name__ == '__main__':
    p = Processor()
    p.process_file()
