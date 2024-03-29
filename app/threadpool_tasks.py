"""
A module that contains functions for performing data analysis tasks.
The functions in this module are intended to be executed asynchronously
"""

import json
import os

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

def _get_mean(data : list, column : str = DATA_COL) -> float:
    valid_data = [float(row.get(column)) for row in data if row.get(column) != '']
    return sum(valid_data) / len(valid_data) if valid_data else float('NaN')

def _check_valid_question(data : dict, data_ingestor : DataIngestor) -> bool:
    return 'question' in data and (data['question'] in data_ingestor.questions_best_is_min or \
            data['question'] in data_ingestor.questions_best_is_max)

def _write_result(job_id : int, result : dict):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w', encoding='utf-8') as file:
        file.write(json.dumps(result))


def states_mean(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculate the mean value for each state in the given data.
    Writes result to a file with the job_id as the filename.

    Args:
        job_id (int): The ID of the job.
        data (dict): The data containing the question and relevant information.
        data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
        None
    """
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        state_means = {state: _get_mean(data) for state, data in data_per_state.items()}
        state_means = {k: v for k, v in sorted(state_means.items(), key=lambda item: item[1])}
        _write_result(job_id, state_means)
    else:
        _write_result(job_id, INVALID_QUESTION)


def state_mean(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculate the mean of a specific state's data for a given question.
    Writes result to a file with the job_id as the filename.

    Parameters:
    - job_id (int): The ID of the job.
    - data (dict): The data dictionary containing the question and state.
    - data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
    None
    """
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


def top(job_id: int, data: dict, data_ingestor: DataIngestor, best: bool, num_top_states: int = 5):
    """
    Retrieves the top states based on the mean value of the data for a given question.
    Writes result to a file with the job_id as the filename.

    Args:
        job_id (int): The ID of the job.
        data (dict): The data dictionary containing the question and relevant data.
        data_ingestor (DataIngestor): An instance of the DataIngestor class.
        best (bool): Flag indicating whether the top states should be based on the highest
            or lowest mean value.
        num_top_states (int, optional): The number of top states to retrieve. Defaults to 5.

    Returns:
        None
    """
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        state_means = {state: _get_mean(data) for state, data in data_per_state.items()}
        # Sort states by mean
        order = question in data_ingestor.questions_best_is_max  \
                if best else question in data_ingestor.questions_best_is_min
        sorted_states = sorted(state_means, key=state_means.get, reverse=order)
        result = {state: state_means[state] for state in sorted_states[:num_top_states]}
        _write_result(job_id, result)
    else:
        _write_result(job_id, INVALID_QUESTION)


def global_mean(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculate the global mean for a given question and write the result.
    Writes result to a file with the job_id as the filename.

    Parameters:
    job_id (int): The ID of the job.
    data (dict): The data containing the question.
    data_ingestor (DataIngestor): The data ingestor object.

    Returns:
    None
    """
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        mean = _get_mean(relevant_data)
        _write_result(job_id, {"global_mean": mean})
    else:
        _write_result(job_id, INVALID_QUESTION)


def diff_from_mean(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculate the difference from the mean for each state in the given data.
    Writes result to a file with the job_id as the filename.

    Parameters:
    - job_id (int): The ID of the job.
    - data (dict): The data containing the question and relevant information.
    - data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
    None
    """
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        mean_global = _get_mean(relevant_data)
        diff = {state: (mean_global - _get_mean(data)) for state, data in data_per_state.items()}
        _write_result(job_id, diff)
    else:
        _write_result(job_id, INVALID_QUESTION)


def state_diff_from_mean(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculates the difference between the mean of a specific state's data and the global mean.
    Writes result to a file with the job_id as the filename.

    Parameters:
    - job_id (int): The ID of the job.
    - data (dict): The data dictionary containing the question and state.
    - data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
    None
    """
    if _check_valid_question(data, data_ingestor) and 'state' in data:
        question = data['question']
        state = data['state']
        relevant_data = data_ingestor.get_data_for_question(question)
        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        mean_global = _get_mean(relevant_data)
        if state not in data_per_state:
            _write_result(job_id, INVALID_STATE)
            return
        diff = mean_global - _get_mean(data_per_state[state])
        _write_result(job_id, {state: diff})
    else:
        _write_result(job_id, INVALID_QUESTION)


def mean_by_category(job_id : int, data : dict, data_ingestor : DataIngestor):
    """
    Calculate the mean value for each category in the given data, grouped by state
        and stratification value.
    Writes result to a file with the job_id as the filename.

    Parameters:
    - job_id (int): The ID of the job.
    - data (dict): The data dictionary containing the question and relevant data.
    - data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
    None
    """
    if _check_valid_question(data, data_ingestor):
        question = data['question']
        relevant_data = data_ingestor.get_data_for_question(question)

        data_per_state = _separate_data_per_column(relevant_data, STATE_COL)
        date_per_state_per_category = {state: _separate_data_per_column(data, STRAT_COL) \
                for state, data in data_per_state.items()}

        result = {f'(\'{state_name}\', \'{data[0][CATEGORY_COL]}\', \'{stratication_value}\')' \
                : _get_mean(data) \
                for state_name, state_data in date_per_state_per_category.items() \
                for stratication_value, data in state_data.items() if stratication_value != ''}
        _write_result(job_id, result)
    else:
        _write_result(job_id, INVALID_QUESTION)


def state_mean_by_category(job_id: int, data: dict, data_ingestor: DataIngestor):
    """
    Calculate the mean value for each category in a specific state.
    Writes result to a file with the job_id as the filename.

    Args:
        job_id (int): The ID of the job.
        data (dict): The data containing the question and state.
        data_ingestor (DataIngestor): An instance of the DataIngestor class.

    Returns:
        None
    """
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
                for stratication_value, data in date_per_category.items()
                if stratication_value != ''}

        sorted_result = {k: result[k] for k in sorted(result)}

        _write_result(job_id, {state: sorted_result})
    else:
        _write_result(job_id, INVALID_QUESTION)
