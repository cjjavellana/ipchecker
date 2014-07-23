#!~/.venv/bin/python

from Queue import Queue
from sets import Set

import sys
import threading
import geoip

lock = threading.Lock()

class Task:
    '''
    An instance of a task to be consumed by the IPChecker thread
    '''

    def __init__(self, ip_addr, time):
        self.ip_addr = ip_addr
        self.time = time

class IPChecker(threading.Thread):
    '''
    Checks the country of origin of a given IP Address
    '''

    def __init__(self, thread_id, task_queue, collector_set):
        super(IPChecker, self).__init__()
        self.task_queue = task_queue
        self.thread_id = thread_id
        self.collector_set = collector_set

    def run(self):
        try:
            while True:
                task = self.task_queue.get()
                                
                country = geoip.country(task.ip_addr)
                print '[Thread: %s] Popped Ip: %s, Time: %s, Country: %s' % \
                    (self.thread_id, task.ip_addr, task.time, country)
                
                if country is 'HK':
                    with lock:
                        self.collector_set.add(task.ip_addr)

                self.task_queue.task_done();

        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise

def main():
    '''
    Prep the queue
    '''
    queue = Queue()
    collector_set = Set()

    '''
    Instantiate 20 Threads
    '''
    for x in range(0, 20):
        t = IPChecker(thread_id=x, task_queue=queue, collector_set=collector_set)
        t.daemon = True
        t.start()

    '''
    Open the apache logs
    '''
    with open('access_log_20140719') as f:
        for line in f:
            ip = str.split(line)
            task = Task(ip_addr=ip[1], time=ip[5][1:])
            queue.put(task)

    '''
    Wait for threads to finish
    '''
    queue.join()

    print '** Processing Complete. Printing result **'

    for v in collector_set:
        print '%s' % (v)

if __name__ == '__main__':
    main()
