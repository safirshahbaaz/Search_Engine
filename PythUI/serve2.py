from flask import Flask
from flask import request
from flask import render_template
import sys

sys.path.insert(0, '../QueryProcessor')

import QueryProcessor

app = Flask(__name__)

@app.route('/')
def my_form():
    return render_template("my-form.html")

@app.route('/', methods=['POST'])
def my_form_post():

    text = request.form['text'] # contains the query submitted

    results = ["http://www.ics.uci.edu", "http://facebook.com"]

    # keep result in the form of a string, should be fine for small number of results
    # make sure multiple results are concatenated by using <br> so html displays them in new lines
    
    return render_template("results.html", name = results)  #return what you want here

if __name__ == '__main__':
    #return app.root_path
    app.debug= True
    app.run()
