from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
import time
import random

futurelist = []

def jobHandler(sleeptime):
    print("jobHandler: %d" % sleeptime)
    time.sleep(sleeptime)
    return sleeptime

def jobCompleted(future):
    futurelist.remove(future)
    print("jobCompleted: %s, list size:%s" % (future.result(), len(futurelist)))

def testpool():
    pool = ThreadPoolExecutor(10)
    timelist = []
    for i in range(1, 10):
        timelist.append(random.randint(10, 50))
    for i in timelist:
        f = pool.submit(jobHandler, i)
        futurelist.append(f)
        f.add_done_callback(jobCompleted)
            # print(len(reslist))

if __name__ == "__main__":
    testpool()