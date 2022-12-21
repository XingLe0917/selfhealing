from functools import total_ordering

@total_ordering
class Wbxtask:
    def __init__(self, priority, task):
        self._priority = priority
        self._task = task

    def __eq__(self, other):
        return self._priority == other._priority

    def __lt__(self, other):
        return self._priority > other._priority

    def __gt__(self, other):
        return self._priority < other._priority

    def __ne__(self, other):
        return self._priority != other._priority

    def __str__(self):
        return "priority=%s, task=%s" % (self._priority, self._task)
