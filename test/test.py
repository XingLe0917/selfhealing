from queue import PriorityQueue

from common.wbxtask import Wbxtask

if __name__ == "__main__":
    q = PriorityQueue()
    q.put(Wbxtask(1,{'taskid': '1', 'priority': 1, 'task_type': 'INFLUXDB_ISSUE_TASK'}))
    q.put(Wbxtask(1,{'taskid': '2', 'priority': 1, 'task_type': 'INFLUXDB_ISSUE_TASK'}))
    q.put(Wbxtask(4,{'taskid': '3', 'priority': 4, 'task_type': 'INFLUXDB_ISSUE_TASK'}))
    q.put(Wbxtask(1,{'taskid': '4', 'priority': 1, 'task_type': 'INFLUXDB_ISSUE_TASK'}))
    print(q.qsize())
    while not q.empty():
        task = q.get(timeout=10)
        print(task)