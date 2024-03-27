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
        
    def submit(self, fn, *args, **kwargs):
        future = self.executor.submit(fn, *args, **kwargs)
        with self.dict_lock:
            self.futures[future] = time.time()
            
        return future
    
    def check_job(self, job_id):
        with self.dict_lock:
            if self.futures[job_id].done():
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
            for future in list(self.thread_pool.futures.keys()):
                if future.done():
                    self.thread_pool.futures.pop(future)
