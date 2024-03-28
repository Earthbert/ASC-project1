import logging
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
import time
import os

CLEANUP_INTERVAL = 2

class ThreadPool:
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

    def submit(self, fn, job_id, *args, **kwargs):
        future = self.executor.submit(fn, *args, **kwargs)
        with self.dict_lock:
            self.futures[job_id] = future

    def get_jobs(self, max_jobs):
        result : dict = {}
        with self.dict_lock:
            for i in range(1, max_jobs):
                if i in self.futures and not self.futures[i].done():
                    result[f"job_id_{i}"] = "running"
                else:
                    result[f"job_id_{i}"] = "done"
                    if i in self.futures:
                        if self.futures[i].exception():
                            logging.info(self.futures[i].exception())
                        self.futures.pop(i)

    def check_job(self, job_id : int) -> bool:
        with self.dict_lock:
            if job_id not in self.futures or self.futures[job_id].done():
                if job_id in self.futures:
                    if self.futures[job_id].exception():
                            logging.info(self.futures[job_id].exception())
                    self.futures.pop(job_id)
                return True
        return False
    
    def shutdown(self):
        self.executor.shutdown()
        self.cleaner.join()


class ThreadPoolCleaner(Thread):
    def __init__(self, thread_pool: ThreadPool):
        super().__init__()
        self.thread_pool = thread_pool

    def run(self):
        while True:
            time.sleep(CLEANUP_INTERVAL)
            self.cleanup_futures()
            
    def cleanup_futures(self):
        with self.thread_pool.dict_lock:
            for i, future in list(self.thread_pool.futures.items()):
                if future.done():
                    if self.futures[i].exception():
                            logging.info(self.futures[i].exception())
                    self.thread_pool.futures.pop(i)
