from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import logging

logging.basicConfig(filename='webserver.log', filemode='w', level=logging.INFO)

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

webserver.running = True

from app import routes
