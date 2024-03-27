from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import logging

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

logging.basicConfig(filename='webserver.log', filemode='w', level=logging.INFO)

from app import routes
