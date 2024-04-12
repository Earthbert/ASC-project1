"""
    This module contains the routes for the webserver.
"""
import os
import json
import logging

from flask import request, jsonify
from app import webserver, threadpool_tasks

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
GREAT_SUCCESS = 200

def _handle_request(task: callable, *args):
    """
    Handles a request by executing the specified task asynchronously.

    Args:
        task (callable): The task to be executed.
        *args: Additional arguments to be passed to the task.

    Returns:
        Flask.Response: The response containing the job ID associated with the request.
    """
    data = request.json
    request_path = request.path
    logging.info("Got request at %s with data:\n %s", request_path, data)
    with webserver.job_counter_lock:
        current_job_counter = webserver.job_counter
        webserver.job_counter += 1
    webserver.tasks_runner.submit(task, current_job_counter, data, webserver.data_ingestor, *args)
    result = jsonify({"job_id": current_job_counter})
    return result


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id: str):
    """
    Get the response for a given job ID.

    Args:
        job_id (str): The ID of the job.

    Returns:
        tuple: A tuple containing the JSON response and the HTTP status code.

    """
    logging.info("Got request for job_id %s", job_id)

    if webserver.running is False:
        return jsonify({'status': 'shutting down'}), GREAT_SUCCESS

    # Check if job_id is valid
    if int(job_id) >= webserver.job_counter:
        return jsonify({'status': 'error', 'reason': 'Invalid job_id'})

    # Check if job is done
    if webserver.tasks_runner.check_job(int(job_id)):
        file_path = os.path.join(RESULTS_DIR, f"{job_id}")
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                res = json.load(file)
                return jsonify({'status': 'done', 'data': res}), GREAT_SUCCESS
        else:
            return jsonify({'status': 'error', 'reason': 'File not found'}), GREAT_SUCCESS
    return jsonify({'status': 'running'}), GREAT_SUCCESS


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Handle the request for calculating the mean of states.

    Returns:
        A tuple containing the result of the request and the status code.
    """
    return _handle_request(threadpool_tasks.states_mean), GREAT_SUCCESS


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    Handles the request for calculating the mean of the state.

    Returns:
        A tuple containing the result of the request and the status code.
    """
    return _handle_request(threadpool_tasks.state_mean), GREAT_SUCCESS


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    Handle the best5_request and return the result.

    Returns:
        tuple: A tuple containing the result of the request and the success status.
    """
    return _handle_request(threadpool_tasks.top, True), GREAT_SUCCESS


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    Handle the worst5_request and return the result.

    Returns:
        The result of the _handle_request function and the constant GREAT_SUCCESS.
    """
    return _handle_request(threadpool_tasks.top, False), GREAT_SUCCESS


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    Handle the global mean request.

    Returns:
        A tuple containing the result of the _handle_request function and the GREAT_SUCCESS status.
    """
    return _handle_request(threadpool_tasks.global_mean), GREAT_SUCCESS


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Handles the request for calculating the difference from the mean.

    Returns:
        A tuple containing the result of the request and the status code.
    """
    return _handle_request(threadpool_tasks.diff_from_mean), GREAT_SUCCESS


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    Handles the state difference from mean request.

    This function calls the _handle_request function with the task 'state_diff_from_mean'
    from the threadpool_tasks module.

    Returns:
        A tuple containing the result of the _handle_request function and the status code
        'GREAT_SUCCESS'.
    """
    return _handle_request(threadpool_tasks.state_diff_from_mean), GREAT_SUCCESS


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    Handles the request for calculating the mean by category.

    Returns:
        A tuple containing the result of the request and the status code.
    """
    return _handle_request(threadpool_tasks.mean_by_category), GREAT_SUCCESS


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Handles the request for state mean by category.

    Returns:
        A tuple containing the result of the request and the status code.
    """
    return _handle_request(threadpool_tasks.state_mean_by_category), GREAT_SUCCESS


@webserver.route('/api/gracefull_shutdown', methods=['GET'])
def gracefull_shutdown():
    """
    Gracefully shuts down the web server.

    This function sets the `running` flag of the web server to False and
    shuts down the tasks runner. It returns a JSON response indicating
    that the server is shutting down.

    Returns:
        A JSON response with the status message and the HTTP status code.
    """
    webserver.running = False
    webserver.tasks_runner.shutdown()
    return jsonify({'status': 'shutting down'}), GREAT_SUCCESS


@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """
    Returns the number of jobs in the webserver's task runner.

    Returns:
        A JSON response containing the number of jobs and a success status.
    """
    result = len(webserver.tasks_runner.get_jobs(webserver.job_counter))
    return jsonify({'num_jobs': result}), GREAT_SUCCESS


@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """
    Retrieve the jobs from the webserver's tasks runner.

    Returns:
        A JSON response containing the result of the jobs and a success status code.
    """
    result = webserver.tasks_runner.get_jobs(webserver.job_counter)
    return jsonify(result), GREAT_SUCCESS


@webserver.route('/')
@webserver.route('/index')
def index():
    """
    Renders the index page of the webserver.

    Returns:
        str: The HTML content of the index page.
    """
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    """
    Returns a list of defined routes in the webserver.

    Returns:
        list: A list of strings representing the defined routes,
            along with the HTTP methods allowed for each route.
    """
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
