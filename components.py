import heapq
import itertools
import random

from raft_protocol import LogEntry

class PersistentState:
  def __init__(self):
    self.current_term = 0
    self.voted_for = None
    self.log = [LogEntry(0, 0, None)]
  def GetCurrentTerm(self):
    return self.current_term
  def SetCurrentTerm(self, current_term):
    assert(self.current_term < current_term)
    self.voted_for = None
    self.current_term = current_term
  def SetVotedFor(self, term, voted_for):
    assert(self.current_term == term)
    assert(self.voted_for == None or self.voted_for == voted_for)
    self.voted_for = voted_for
  def GetVotedFor(self):
    return self.voted_for
  def GetLastLogIndexAndTerm(self):
    entry = self.log[-1]
    return (entry.index, entry.term)
  def GetLogEntry(self, index):
    if index >= len(self.log):
      return None
    return self.log[index]
  def AppendLogEntry(self, entry):
    assert(len(self.log) == entry.index)
    self.log.append(entry)
  def DeleteLogEntriesAfterIndex(self, index):
    del self.log[index+1:]

class MessageQueue:
  def __init__(self):
    self.current_time = 0
    self.counter = itertools.count()
    self.task_queue = []

  def Now(self):
    return self.current_time
  def PostDelayedTask(self, delay, task):
    time = self.Now() + delay
    self.PostTaskForTime(time, task)
    return time
  def PostTask(self, task):
    self.PostTaskForTime(self.Now(), task)
  def PostTaskForTime(self, time, task):
    entry = (time, next(self.counter), task)
    heapq.heappush(self.task_queue, entry)
  def RunOneTask(self):
    time, _, task = heapq.heappop(self.task_queue)
    self.current_time = time
    task()
  def RunUntilIdle(self):
    while len(self.task_queue) > 0:
      self.RunOneTask()
  def RunUntilTime(self, time):
    while True:
      if not self.task_queue:
        break
      task_time, _, _ = self.task_queue[0]
      if task_time > time:
        break
      self.RunOneTask()
    self.current_time = time

class Network:
  def __init__(self, message_queue):
    self.message_queue = message_queue
    self.replicas = {}
    self.policy = self.PolicyDeliverMessageImmediately
  def SetDeliveryPolicy(self, policy):
    self.policy = policy
  def RegisterReplica(self, replica_id, handler):
    self.replicas[replica_id] = handler
  def UnregisterReplica(self, replica_id):
    del self.replicas[replica_id]
  def SendMessage(self, src, dst, msg):
    self.policy(self, src, dst, msg)
  @staticmethod
  def PolicyDeliverMessageImmediately(network, src, dst, msg):
    network.ScheduleDelayedMessageDelivery(0, src, dst, msg)
  def ScheduleDelayedMessageDelivery(self, delay, src, dst, msg):
    time = self.message_queue.PostDelayedTask(
        delay, lambda: self.DeliverMessage(src, dst, msg))
  def DeliverMessage(self, src, dst, msg):
    # print("MSG:t=%d:%s=>%s:%s"%(self.message_queue.Now(), src, dst, msg))
    handler = self.replicas[dst]
    handler.HandleNetworkMessage(src, msg)
