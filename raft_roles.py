from raft_protocol import AppendEntriesResponse, \
    RequestVoteRequest, RequestVoteResponse

# Follower
class FollowerDelegate:
  def UpdateCommitIndex(self, commit_index):
    pass
  def FollowerAcceptedRequest(self, leader_id):
    pass

class Follower:
  def __init__(self, persistent_state, follower_delegate):
    self.follower_delegate = follower_delegate
    self.persistent_state = persistent_state
    self.commit_index = 0

    self.term = self.persistent_state.GetCurrentTerm()

  def ShouldAcceptVote_(self, request):
    if request.term < self.term:
      return False
    assert(request.term == self.term)
    voted_for = self.persistent_state.GetVotedFor()
    if voted_for != None and voted_for != request.candidate_id:
      return False
    (last_log_index, last_log_term) = \
        self.persistent_state.GetLastLogIndexAndTerm()
    if request.last_log_term < last_log_term:
      return False
    if request.last_log_term == last_log_term and \
       request.last_log_index < last_log_index:
      return False
    return True

  def HandleRequestVoteRequest(self, request):
    should_accept_vote = self.ShouldAcceptVote_(request)
    if should_accept_vote:
      self.persistent_state.SetVotedFor(request.term, request.candidate_id)
      self.follower_delegate.FollowerAcceptedRequest(None)
    return RequestVoteResponse(self.term, should_accept_vote)

  def ShouldAppendEntries_(self, request):
    if request.term < self.term:
      return False
    assert(request.term == self.term)
    entry = self.persistent_state.GetLogEntry(request.prev_log_index)
    if entry == None:
      return False
    assert(entry.index == request.prev_log_index)
    if entry.term != request.prev_log_term:
      return False
    return True

  def AppendEntries_(self, request):
    entries_match = True
    if request.entries:
      last_remote_entry = request.entries[-1]
      local_entry = self.persistent_state.GetLogEntry(last_remote_entry.index)
      if not local_entry or local_entry.term != last_remote_entry.term:
        entries_match = False

    if not entries_match:
      self.persistent_state.DeleteLogEntriesAfterIndex(request.prev_log_index)
      for entry in request.entries:
        self.persistent_state.AppendLogEntry(entry)

    last_log_index, _ = self.persistent_state.GetLastLogIndexAndTerm()
    can_commit_up_to = min(request.leader_commit_index, last_log_index)
    if self.commit_index < can_commit_up_to:
      self.commit_index = can_commit_up_to
      self.follower_delegate.UpdateCommitIndex(self.commit_index)
    return request.prev_log_index + len(request.entries)

  def HandleAppendEntriesRequest(self, request):
    index = request.prev_log_index
    should_append_entries = self.ShouldAppendEntries_(request)
    if should_append_entries:
      index = self.AppendEntries_(request)
      self.follower_delegate.FollowerAcceptedRequest(request.leader_id)
    return AppendEntriesResponse(self.term, should_append_entries,
        index, request.message_id)


# Candidate
class CandidateDelegate:
  def BecameLeader(self):
    pass

class Candidate:
  def __init__(self, replica_set, replica_id,
        persistent_state, network, delegate):
    self.replica_set = replica_set
    self.replica_id = replica_id
    self.persistent_state = persistent_state
    self.network = network
    self.delegate = delegate
    self.StartElection_()

  def StartElection_(self):
    self.election_term = self.persistent_state.GetCurrentTerm() + 1
    self.persistent_state.SetCurrentTerm(self.election_term)
    self.persistent_state.SetVotedFor(self.election_term, self.replica_id)
    self.accepted_replicas = {self.replica_id}
    self.SendRequestVoteRequests_()

  def SendRequestVoteRequests_(self):
    other_replicas = self.replica_set - {self.replica_id}
    last_log_index, last_log_term = \
        self.persistent_state.GetLastLogIndexAndTerm()
    msg = RequestVoteRequest(
        self.election_term, self.replica_id, last_log_index, last_log_term)
    for id in other_replicas:
      self.network.SendMessage(self.replica_id, id, msg)

  def HandleRequestVoteResponse(self, src, msg):
    assert(msg.term == self.election_term)
    if not msg.vote_granted:
      return
    assert(src in self.replica_set)
    assert(src != self.replica_id)
    self.accepted_replicas.add(src)
    if len(self.accepted_replicas) > len(self.replica_set) / 2:
      self.delegate.BecameLeader()
