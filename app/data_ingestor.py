"""
A module that contains a class for ingesting data from a CSV file.
"""

import csv

class DataIngestor:
    """
    A class that represents a data ingestor for CSV files.

    Attributes:
        data (list): A list of dictionaries representing the data from the CSV file.
        questions_best_is_min (list): A list of questions where the best value is the minimum.
        questions_best_is_max (list): A list of questions where the best value is the maximum.
    """

    def __init__(self, csv_path: str):
        """
        Initializes a DataIngestor object.

        Args:
            csv_path (str): The path to the CSV file.

        """
        # Read csv from csv_path
        with open(csv_path, 'r') as file:
            self.data = list(csv.DictReader(file))

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

    def get_data_for_question(self, question: str) -> list:
        """
        Retrieves data rows that match the given question.

        Args:
            question (str): The question to match.

        Returns:
            list: A list of data rows that match the given question.
        """
        return [row for row in self.data if row['Question'] == question]
