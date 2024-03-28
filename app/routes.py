from app import webserver
from flask import request, jsonify

import app.threadpool_tasks as threadpool_tasks
import logging

import os
import json

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id : str):
    logging.info(f"Got request for job_id {job_id}")

    if webserver.running == False:
        return jsonify({'status': 'shutting down'}), 200

    # Check if job_id is valid
    if int(job_id) >= webserver.job_counter:
        return jsonify({'status': 'error', 'reason': 'Invalid job_id'})

    # Check if job is done
    if webserver.tasks_runner.check_job(int(job_id)):
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'r') as f:
            res = json.load(f)
        return jsonify({'status': 'done', 'data': res}), 200
    else:
        return jsonify({'status': 'running'}), 200

def _handle_request(task : callable, *args):
     # Get request data
    data = request.json
    logging.info(f"Got request {data}")
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.submit(task, webserver.job_counter, data, webserver.data_ingestor, *args)
    # Increment job_id counter
    result = jsonify({"job_id": webserver.job_counter})
    webserver.job_counter += 1
    # Return associated job_id
    return result

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    return _handle_request(threadpool_tasks.states_mean), 200

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    return _handle_request(threadpool_tasks.state_mean), 200

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    return _handle_request(threadpool_tasks.top, True), 200

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    return _handle_request(threadpool_tasks.top, False), 200

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    return _handle_request(threadpool_tasks.global_mean), 200

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    return _handle_request(threadpool_tasks.diff_from_mean), 200

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    return _handle_request(threadpool_tasks.state_diff_from_mean), 200

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    return _handle_request(threadpool_tasks.mean_by_category), 200

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    return _handle_request(threadpool_tasks.state_mean_by_category), 200

@webserver.route('/api/gracefull_shutdown', methods=['GET'])
def gracefull_shutdown():
    webserver.running = False
    webserver.tasks_runner.shutdown()
    return jsonify({'status': 'shutting down'}), 200

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
