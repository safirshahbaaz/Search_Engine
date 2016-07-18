from flask import Flask
from flask import request,render_template, url_for
import sys

import Retriever

app = Flask(__name__)

@app.route('/')
def my_form():
    return render_template("my-form.html")

@app.route('/', methods=['POST'])
def my_form_post():

    query = request.form['text'].encode('utf-8') # contains the query submitted

    results = Retriever.runner(query)

    # keep result in the form of a string, should be fine for small number of results
    # make sure multiple results are concatenated by using <br> so html displays them in new lines
    
    return render_template("results.html", name = results)  #return what you want here

if __name__ == '__main__':
    #return app.root_path
    app.debug= True
    app.run(port=5004)
