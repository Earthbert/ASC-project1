import json
import os
import logging

from app.data_ingestor import DataIngestor

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
INVALID_QUESTION = {"error": "Invalid question"}
INVALID_STATE = {"error": "Invalid state"}

STATE_COL = 'LocationDesc'
CATEGORY_COL = 'StratificationCategory1'
STRAT_COL = 'Stratification1'
DATA_COL = 'Data_Value'

def _separate_data_per_column(data : list, column) -> dict:
    state_data = {}
    for row in data:
        location_desc = row.get(column)
        if location_desc not in state_data:
            state_data[location_desc] = []
        state_data[location_desc].append(row)
    return state_data

def _get_mean(data : list) -> float:
    valid_data = [float(row.get(DATA_COL)) for row in data if row.get(DATA_COL) != '']
    return sum(valid_data) / len(valid_data) if valid_data else float('nan')

def _check_valid_question(data : dict, data_ingestor : DataIngestor) -> bool:
    return 'question' in data and (data['question'] in data_ingestor.questions_best_is_min or data['question'] in data_ingestor.questions_best_is_max)

def _write_result(job_id : int, result : dict):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
        f.write(json.dumps(result))

def states_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        state_means = {state: _get_mean(data) for state, data in data_per_state.items()}
        _write_result(job_id, state_means)
    else:
        _write_result(job_id, INVALID_QUESTION)
            
def state_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor) and 'state' in data:
        question = data['question']
        state = data['state']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)

        if state not in data_per_state:
            _write_result(job_id, INVALID_STATE)
        result = {state: _get_mean(data_per_state[state])}
        _write_result(job_id, result)
    else:
        _write_result(job_id, INVALID_QUESTION)

def top(job_id : int, data : dict, data_ingestor : DataIngestor, best : bool, n : int = 5):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        state_means = {state: _get_mean(data) for state, data in data_per_state.items()}
        # Sort states by mean
        order = question in data_ingestor.questions_best_is_max if best else question in data_ingestor.questions_best_is_min
        sorted_states = sorted(state_means, key=state_means.get, reverse=order)
        result = {state: state_means[state] for state in sorted_states[:n]}
        _write_result(job_id, result)
    else:
        _write_result(job_id, INVALID_QUESTION)

def global_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        mean = _get_mean(relevant_data)
        _write_result(job_id, {"global_mean": mean})
    else:
        _write_result(job_id, INVALID_QUESTION)
            
def diff_from_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        global_mean = _get_mean(relevant_data)
        diff = {state : (global_mean - _get_mean(data)) for state, data in data_per_state.items()}
        _write_result(job_id, diff)
    else:
        _write_result(job_id, INVALID_QUESTION)

def state_diff_from_mean(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor) and 'state' in data:
        question = data['question']
        state = data['state']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        global_mean = _get_mean(relevant_data)
        if state not in data_per_state:
            _write_result(job_id, INVALID_STATE)
            return
        diff = global_mean - _get_mean(data_per_state[state])
        _write_result(job_id, {state : diff})
    else:
        _write_result(job_id, INVALID_QUESTION)
        
def mean_by_category(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        date_per_state_per_category = {state: _separate_data_per_column(data, STRAT_COL) \
            for state, data in data_per_state.items()}

        result = {f'(\'{state_name}\', \'{data[0][CATEGORY_COL]}\', \'{stratication_value}\')' : _get_mean(data) \
            for state_name, state_data in date_per_state_per_category.items() \
            for stratication_value, data in state_data.items()}
        _write_result(job_id, result)
    else:
        _write_result(job_id, INVALID_QUESTION)

def state_mean_by_category(job_id : int, data : dict, data_ingestor : DataIngestor):
    # Process data
    if _check_valid_question(data, data_ingestor) and 'state' in data:
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        state = data['state']
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        if state not in data_per_state:
            _write_result(job_id, INVALID_STATE)
            return
        date_per_category = _separate_data_per_column(data_per_state[state], STRAT_COL)

        result = {f'(\'{data[0][CATEGORY_COL]}\', \'{stratication_value}\')' : _get_mean(data) \
            for stratication_value, data in date_per_category.items()}
        _write_result(job_id, {state: str(result)})
    else:
        _write_result(job_id, INVALID_QUESTION)