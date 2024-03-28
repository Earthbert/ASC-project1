import logging
from threading import Thread, Lock
from concurrent.futures import Future, ThreadPoolExecutor
import time
import os

CLEANUP_INTERVAL = 2

def _log_exception(future : Future, job_id : int):
    if future.exception():
        logging.info("Job %d failed with exception: %s", job_id, future.exception())

class ThreadPool:
    """
    A thread pool implementation for executing jobs asynchronously.

    The ThreadPool class provides methods for submitting jobs to be executed
    asynchronously and retrieving the status of running and completed jobs.

    Attributes:
        executor (ThreadPoolExecutor): The ThreadPoolExecutor instance used for
            executing the jobs.
        futures (dict): A dictionary that maps job IDs to corresponding Future
            objects representing the execution of the jobs.
        dict_lock (Lock): A lock used for thread-safe access to the futures dictionary.
        cleaner (ThreadPoolCleaner): A ThreadPoolCleaner instance responsible for
            cleaning up completed jobs.
    """

    def __init__(self):
        # Check if TP_NUM_OF_THREADS environment variable is defined
        if 'TP_NUM_OF_THREADS' in os.environ:
            num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        else:
            # Use hardware concurrency if TP_NUM_OF_THREADS is not defined
            num_threads = os.cpu_count()

        # Create a ThreadPoolExecutor with the specified number of threads
        self.executor : ThreadPoolExecutor = ThreadPoolExecutor(max_workers=num_threads)
        self.futures : dict = {}
        self.dict_lock : Lock = Lock()
        
        self.cleaner = ThreadPoolCleaner(self)
        self.cleaner.start()

    def submit(self, fn: callable, job_id: int, *args, **kwargs):
            """
            Submits a job to be executed asynchronously.

            Args:
                fn (callable): The function to be executed.
                job_id (int): The ID of the job.
                *args: Variable length argument list to be passed to the function.
                **kwargs: Arbitrary keyword arguments to be passed to the function.

            Returns:
                None
            """
            future = self.executor.submit(fn, job_id, *args, **kwargs)
            with self.dict_lock:
                self.futures[job_id] = future

    def get_jobs(self, max_jobs: int) -> dict:
            """
            Retrieve the status of the running and completed jobs.

            Args:
                max_jobs (int): The maximum number of jobs to retrieve.

            Returns:
                dict: A dictionary containing the status of the jobs. The keys are in the format "job_id_{job_id}"
                      and the values are either "running" or "done".
            """
            result: dict = {}
            with self.dict_lock:
                for job_id in range(1, max_jobs):
                    if job_id in self.futures and not self.futures[job_id].done():
                        result[f"job_id_{job_id}"] = "running"
                    else:
                        result[f"job_id_{job_id}"] = "done"
                        if job_id in self.futures:
                            _log_exception(self.futures[job_id], job_id)
                            self.futures.pop(job_id)
            return result

    def check_job(self, job_id : int) -> bool:
            """
            Checks the status of a job with the given job_id.

            Args:
                job_id (int): The ID of the job to check.

            Returns:
                bool: True if the job is completed or not found, False otherwise.
            """
            with self.dict_lock:
                if job_id not in self.futures or self.futures[job_id].done():
                    if job_id in self.futures:
                        _log_exception(self.futures[job_id], job_id)
                        self.futures.pop(job_id)
                    return True
            return False
    
    def shutdown(self):
        """
        Shuts down the task runner by stopping the executor and joining the cleaner thread.
        """
        self.executor.shutdown()
        self.cleaner.join()


class ThreadPoolCleaner(Thread):
    """
    A class that represents a thread responsible for cleaning up completed futures in a ThreadPool.

    Attributes:
        thread_pool (ThreadPool): The ThreadPool instance associated with this cleaner thread.
    """

    def __init__(self, thread_pool: ThreadPool):
        super().__init__()
        self.thread_pool = thread_pool

    def run(self):
        """
        The main method that runs the thread.

        This method continuously sleeps for a specified interval and then calls the `cleanup_futures` method
        to remove completed futures from the thread pool.
        """
        while True:
            time.sleep(CLEANUP_INTERVAL)
            self.cleanup_futures()
            
    def cleanup_futures(self):
        """
        Removes completed futures from the thread pool.

        This method iterates over the futures in the thread pool and removes any futures that have completed.
        If a completed future is found, it logs the exception associated with the future and removes it from the pool.
        """
        with self.thread_pool.dict_lock:
            for job_id, future in list(self.thread_pool.futures.items()):
                if future.done():
                    _log_exception(future, job_id)
                    self.thread_pool.futures.pop(job_id)
