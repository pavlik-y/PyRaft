from raft_timers import Timer

from raft_protocol import AppendEntriesRequest, LogEntry

class BackoffPolicy:
  def __init__(self, timeout):
    self.timeout = timeout
    self.last_message_id = None
    self.last_message_send_time = None
  def MessageSent(self, message_id, now):
    self.last_message_id = message_id
    self.last_message_send_time = now
  def ResponseReceived(self, message_id, now):
    if message_id != self.last_message_id:
      return
    self.last_message_id = None
    self.last_message_send_time = None
  def GetDelayUntilNextMessage(self, time):
    if self.last_message_id:
      return max(self.last_message_send_time + self.timeout - time, 0)
    else:
      return 0

class MatchIndexFinder:
  def __init__(self, last_index):
    self.anchor = last_index + 1
    self.last_index = last_index
    self.highest_match_index = None
    self.lowest_mismatch_index = self.anchor
  def GetMatchIndex(self):
    if self.highest_match_index == None:
      return None
    if self.highest_match_index != self.lowest_mismatch_index - 1:
      return None
    return self.highest_match_index
  def GetNextIndexToProbe(self):
    assert(self.GetMatchIndex() == None)
    if self.highest_match_index != None:
      delta = self.lowest_mismatch_index - self.highest_match_index
      return self.highest_match_index + delta / 2
    delta = self.anchor - self.lowest_mismatch_index
    delta = max(delta * 2, 1)
    next_index = max(self.anchor - delta, 0)
    return next_index
  def ProbeResultReceived(self, index, is_match):
    assert(index != 0 or is_match)
    if is_match:
      assert(index > self.highest_match_index)
      self.highest_match_index = index
    else:
      assert(index < self.lowest_mismatch_index)
      self.lowest_mismatch_index = index

# ReplicaTracker
class ReplicaTracker:
  def __init__(self, id, last_index, backoff_policy, heartbeat_interval):
    self.id = id
    self.backoff_policy = backoff_policy
    self.heartbeat_interval = heartbeat_interval
    self.last_index = last_index
    self.match_index_finder = MatchIndexFinder(last_index)
    self.match_index = None
    self.last_message_send_time = None
  def MessageSent(self, msg, now):
    self.last_message_send_time = now
    self.backoff_policy.MessageSent(msg.message_id, now)
  def ResponseReceived(self, msg, now):
    self.backoff_policy.ResponseReceived(msg.message_id, now)
    if self.match_index_finder:
      self.match_index_finder.ProbeResultReceived(msg.index, msg.success)
      self.match_index = self.match_index_finder.GetMatchIndex()
      if self.match_index != None:
        self.match_index_finder = None
      return
    assert(msg.success)
    self.match_index = max(self.match_index, msg.index)
  def LastIndexUpdated(self, last_index, now):
    assert(self.last_index <= last_index)
    self.last_index = last_index
  def GetMessageToSend(self, now):
    if self.backoff_policy.GetDelayUntilNextMessage(now) > 0:
      # Message is in flight
      return None
    if self.match_index_finder != None:
      probe_index = self.match_index_finder.GetNextIndexToProbe()
      return (probe_index, probe_index)
    assert(self.match_index != None)
    assert(self.match_index <= self.last_index)
    if self.match_index == self.last_index and \
        (now - self.last_message_send_time) < self.heartbeat_interval:
      # No entries to send and heartbeat time hasn't passed
      return None
    return (self.match_index, self.last_index)
  def GetDelayUntilNextNudge(self, now):
    delay = self.backoff_policy.GetDelayUntilNextMessage(now)
    if delay > 0:
      return delay
    assert(self.last_index == self.match_index)
    delay = max(self.last_message_send_time + self.heartbeat_interval - now, 0)
    assert(delay > 0)
    return delay

# Leader
class IdGenerator:
  def __init__(self):
    self.next_id = 1
  def GetNextId(self):
    id = self.next_id
    self.next_id += 1
    return id

class LeaderDelegate:
  def UpdateCommitIndex(self, commit_index):
    pass

class Leader:
  def __init__(self, replica_set, replica_id,
               network, persistent_state, scheduler,
               message_timeout, heartbeat_interval,
               leader_delegate):
    self.replica_id = replica_id
    self.network = network
    self.persistent_state = persistent_state
    self.leader_delegate = leader_delegate
    self.scheduler = scheduler
    self.message_timeout = message_timeout
    self.heartbeat_interval = heartbeat_interval
    self.timer = Timer(scheduler)
    self.timer.SetCallback(self._HandleReplicas)

    assert(self.persistent_state.GetVotedFor() == replica_id)

    self.term = self.persistent_state.GetCurrentTerm()
    self.last_log_index, _ = self.persistent_state.GetLastLogIndexAndTerm()
    self.commit_index = 0
    self.id_generator = IdGenerator()
    self.replicas = dict(
        (id, self._CreateReplicaTracker(id))
         for id in replica_set if id != replica_id)
    self._HandleReplicas()

  def _CreateReplicaTracker(self, id):
    backoff_policy = BackoffPolicy(self.message_timeout)
    replica_tracker = ReplicaTracker(id, self.last_log_index,
        backoff_policy, self.heartbeat_interval)
    return replica_tracker
  def Stop(self):
    self.timer.Stop()

  def _HandleReplicas(self):
    delay = None
    now = self.scheduler.Now()
    for replica in self.replicas.itervalues():
      replica_delay = self._HandleReplica(replica, now)
      assert(replica_delay > 0)
      if delay == None or delay > replica_delay:
        delay = replica_delay
    self.timer.ResetForInterval(delay)

  def _HandleReplica(self, replica, now):
    while True:
      msg_params = replica.GetMessageToSend(now)
      if msg_params == None:
        break
      match_index, last_index = msg_params
      msg = self._CreateAppendEntriesRequest(match_index, last_index)
      self.network.SendMessage(self.replica_id, replica.id, msg)
      replica.MessageSent(msg, now)
    delay = replica.GetDelayUntilNextNudge(now)
    assert(delay > 0)
    return delay

  def ReplicateCommand(self, cmd):
    self.last_log_index += 1
    self.persistent_state.AppendLogEntry(
        LogEntry(self.last_log_index, self.term, cmd))
    for replica in self.replicas.itervalues():
      replica.LastIndexUpdated(self.last_log_index, self.scheduler.Now())
    self._HandleReplicas()
    return self.last_log_index

  def _CreateAppendEntriesRequest(self, match_index, last_index):
    match_entry = self.persistent_state.GetLogEntry(match_index)
    entries = []
    for index in xrange(match_index+1, last_index+1):
      entries.append(self.persistent_state.GetLogEntry(index))
    request = AppendEntriesRequest(
      self.term, self.replica_id, match_entry.index, match_entry.term,
      entries, self.commit_index, self.id_generator.GetNextId())
    return request

  def HandleAppendEntriesResponse(self, src, msg):
    assert(msg.term == self.term)
    assert(src in self.replicas)
    replica = self.replicas[src]
    replica.ResponseReceived(msg, self.scheduler.Now())
    self.TryUpdateCommitIndex_()
    self._HandleReplicas()

  def TryUpdateCommitIndex_(self):
    match_indices = [r.match_index for r in self.replicas.itervalues()]
    match_indices.append(self.last_log_index)
    match_indices = sorted(match_indices)
    assert(len(match_indices) == len(self.replicas) + 1)
    majority = len(match_indices) / 2 + 1
    majority_match = match_indices[-majority]
    if self.commit_index < majority_match:
      self.commit_index = majority_match
      self.leader_delegate.UpdateCommitIndex(self.commit_index)
