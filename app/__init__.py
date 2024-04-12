from threading import Lock
import time
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import logging, logging.handlers

# Disable werkzeug logs
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# Initialize the webserver
webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

# Set up root logger
logging.basicConfig(level=logging.INFO, filename='webserver.log')

logger = logging.getLogger(None)
handler = logging.handlers.RotatingFileHandler("webserver.log", maxBytes=100000, backupCount=20)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
formatter.converter = time.gmtime
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize the data ingestor
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

# Initialize the job counter and running flag
webserver.job_counter = 1
webserver.job_counter_lock = Lock()

webserver.running = True

from app import routes
