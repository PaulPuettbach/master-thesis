from flask import Flask, request


app = Flask(__name__)
"""
fairness based on queue are all tenants getting teh same resources

locality based on observed message latency is difficult needs to be added to each task

resource utilization dk how to do this one we can get state now and then guestimate i think
enregy efficiency is also resource utilization
"""

"""Ingress"""

#-----this is from the main scheduler-----#
#get updates of workqueue from the main scheduler
@app.route('/workqueue', methods=['POST'])
def workqueue():
    print("update from main scheduler to the workqueue")

# get updates on own resource utilization to change pool size
@app.route('/change_pool', methods=['POST'])
def node_status():
    print("update from main scheduler to the pool")

# get updates on own resource utilization to change pool size
@app.route('/change_pool', methods=['POST'])
def node_status():
    print("update from main scheduler to the pool")

#get the best version from other daemons

"""Logic"""


"""Egress"""
