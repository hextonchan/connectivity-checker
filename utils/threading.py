import os 
import threading
from utils.logging import Logging

LOG = Logging(__name__)

class Log():
    
    def disabled():
        LOG.info('Multithreading disabled.')

if not os.environ.get('MAXIMUM_THREADS'):
    default_threads = 1
    Log.disabled()
else:
    default_threads = [threading.Thread] * int(os.environ.get('MAXIMUM_THREADS'))
    LOG.info('Default number of threads = {}.'.format(str(len(default_threads))))

def thread(target, threads = default_threads):
    ### tightly coupled, shared resources
    for thread_id, thread in enumerate(threads):
        threads[thread_id] = threading.Thread(target=target, args=(thread_id, len(threads)))
        threads[thread_id].start()
        
    [thread.join() for thread_id, thread in enumerate(threads)]
    
class Thread():
    max_thread : int
    threads : list
    
    def __init__(self, max_thread = 2):
        self.max_thread = int(max_thread)
        self.threads = [threading.Thread] * int(max_thread)
        
        LOG.info('Avail Multithreading, maximum threads = {}.'.format(str(len(self.threads))))
    
    def get_max_thread(self):
        return self.max_thread
    
    def start(self, function):
        thread(function, self.threads)