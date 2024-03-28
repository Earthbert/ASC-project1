import json
import os
import logging

from app.data_ingestor import DataIngestor

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')

def _separate_data_per_state(data : list) -> dict:
    state_data = {}
    for row in data:
        location_desc = row.get('LocationDesc')
        if location_desc not in state_data:
            state_data[location_desc] = []
        state_data[location_desc].append(row)
    return state_data

def _get_mean(data : list) -> float:
    valid_data = [float(row.get('Data_Value')) for row in data if row.get('Data_Value') != '']
    return sum(valid_data) / len(valid_data) if valid_data else float('nan')

def _check_valid_question(data : dict, data_ingestor : DataIngestor) -> bool:
    return 'question' in data and (data['question'] in data_ingestor.questions_best_is_min or data['question'] in data_ingestor.questions_best_is_max)

def states_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        separated_data = _separate_data_per_state(relevant_data)
        state_means = {state: _get_mean(data) for state, data in separated_data.items()}
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
            f.write(json.dumps(state_means))
    else:
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
            f.write(json.dumps({"error": "Invalid question"}))
            
def state_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor) and 'state' in data:
        question = data['question']
        state = data['state']
        relevant_data = data_ingestor.get_data_for_question(question)
        separated_data = _separate_data_per_state(relevant_data)
        if state in separated_data:
            result = {state: _get_mean(separated_data[state])}
            with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
                f.write(json.dumps(result))
        else:
            with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
                f.write(json.dumps({"error": "Invalid state"}))
    else:
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
            f.write(json.dumps({"error": "Invalid question"}))

def top(job_id : int, data : dict, data_ingestor : DataIngestor, best : bool, n : int = 5):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        separated_data = _separate_data_per_state(relevant_data)
        state_means = {state: _get_mean(data) for state, data in separated_data.items()}
        # Sort states by mean
        order = question in data_ingestor.questions_best_is_max if best else question in data_ingestor.questions_best_is_min
        sorted_states = sorted(state_means, key=state_means.get, reverse=order)
        result = {state: state_means[state] for state in sorted_states[:n]}
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
            f.write(json.dumps(result))
    else:
        with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
            f.write(json.dumps({"error": "Invalid question"}))


