import collections

# Persistent state

LogEntry = collections.namedtuple("LogEntry", "index, term, cmd")

class PersistentState:
  def GetCurrentTerm(self):
    pass
  def SetCurrentTerm(self, current_term):
    pass
  def GetVotedFor(self):
    pass
  def SetVotedFor(self, term, voted_for):
    pass
  def GetLastLogIndexAndTerm(self):
    pass
  def GetLogEntry(self, index):
    pass
  def AppendLogEntry(self, entry):
    pass
  def DeleteLogEntriesAfterIndex(self, index):
    pass

# Network protocol
RequestVoteRequest = collections.namedtuple(
    'RequestVoteRequest', "term, candidate_id, last_log_index, last_log_term")

RequestVoteResponse = collections.namedtuple(
    'RequestVoteResponse', "term, vote_granted")

AppendEntriesRequestBase = collections.namedtuple(
    'AppendEntriesRequest',
    "term, leader_id, prev_log_index, prev_log_term, entries, " +
    "leader_commit_index, message_id")

class AppendEntriesRequest(AppendEntriesRequestBase):
  def __eq__(self, other):
    if other == None:
      return False
    return all([getattr(self, f) == getattr(other, f)
        for f in self._fields if f != 'message_id'])
  def __ne__(self, other):
    if other == None:
      return True
    return any([getattr(self, f) != getattr(other, f)
        for f in self._fields if f != 'message_id'])

AppendEntriesResponseBase = collections.namedtuple(
    'AppendEntriesResponse', "term, success, index, message_id")

class AppendEntriesResponse(AppendEntriesResponseBase):
  def __eq__(self, other):
    if other == None:
      return False
    return all([getattr(self, f) == getattr(other, f)
        for f in self._fields if f != 'message_id'])
  def __ne__(self, other):
    if other == None:
      return True
    return any([getattr(self, f) != getattr(other, f)
        for f in self._fields if f != 'message_id'])

class Network:
  def SendMessage(self, src, dst, msg):
    pass
