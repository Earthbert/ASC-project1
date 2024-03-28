import json
import os
import logging

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

def states_mean(data, data_ingestor, job_id : int):
	# Process data
	if 'question' in data and (data['question'] in data_ingestor.questions_best_is_min or data['question'] in data_ingestor.questions_best_is_max):
		question = data['question']
		relevant_data = data_ingestor.get_data_for_question(question)
		separated_data = _separate_data_per_state(relevant_data)
		state_means = {state: _get_mean(data) for state, data in separated_data.items()}
		with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
			f.write(json.dumps(state_means))
	else:
		with open(os.path.join(RESULTS_DIR, f"{job_id}"), 'w') as f:
			f.write(json.dumps({"error": "Invalid question"}))

