#!~/.venv/bin/python

from Queue import Queue

import sys
import threading
import geoip

class IPChecker(threading.Thread):
    '''
    Checks the country of origin of a given IP Address
    '''

    def __init__(self, thread_id, task_queue):
        super(IPChecker, self).__init__()
        self.task_queue = task_queue
        self.thread_id = thread_id

    def run(self):
        try:
            while True:
                ip_addr = self.task_queue.get()
                print '[Thread: %s] Popped Ip: %s, Country: %s' % (self.thread_id, ip_addr, geoip.country(ip_addr))
                self.task_queue.task_done()

        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise

def main():
    '''
    Prep the queue
    '''
    queue = Queue()

    '''
    Instantiate 10 Threads
    '''
    for x in range(0, 10):
        t = IPChecker(thread_id=x, task_queue=queue)
        t.daemon = True
        t.start()

    '''
    Open the apache logs
    '''
    with open('access.log') as f:
        for line in f:
            ip = str.split(line)
            #print 'Ip Address: %s' % (ip[0])
            queue.put(ip[0])

    '''
    Wait for threads to finish
    '''
    queue.join()

    print '** Processing Complete **'


if __name__ == '__main__':
    main()
