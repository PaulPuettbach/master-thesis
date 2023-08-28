# this will run on each of the workers
import requests
from flask import Flask, request


#should receive the update to the state of the scheduling
app = Flask(__name__)
@app.route('/', methods=['POST'])
def result():
    print(request.form['foo']) # should display 'bar'
    return 'Received !' # response to your request

#should also post the result after