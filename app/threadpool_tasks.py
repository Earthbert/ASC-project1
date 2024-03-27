from app.routes import RESULTS_DIR

def states_mean(data, data_ingestor):
	# Process data
	if 'question' in data:
		question = data['question']
		relevant_data = data_ingestor.get_relevant_data(question)
