from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import logging

# Set up logging
logging.basicConfig(filename='webserver.log', filemode='w', level=logging.INFO)
werkzeug_logger = logging.getLogger('werkzeug')
# Disable werkzeug logs
werkzeug_logger.setLevel(logging.ERROR)

# Initialize the webserver
webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

# Initialize the data ingestor
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

# Initialize the job counter and running flag
webserver.job_counter = 1
webserver.running = True

from app import routes
