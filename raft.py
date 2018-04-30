# Diagram: http://www.plantuml.com/plantuml/uml/XLLHJy8m47xVhrXyKfBn2um1KPG4ZG7qlRO7JBfrTbsGAF_Tjbl7sXlwGjhlk_lkksEtZKf5fAeo7edt3w0gVW5PfgM2N2qLKH1_Ht7y06fIIQcn5SZiyaf3Impfg3UXW4s5T27UNVDpiRdFbJpO-xWe86SUT0SS53HG2kLuhK3ESWPVcZv6pkDKS2xs8AsvHv8p3WluhA2q1PkZ5Qp9kbAZUQAneAc_SLs_5amV9oBBKkKyn6tec58eT7TzSXDTDJN9djujK9B170W3sHz-hf-o47a9NiuJKNHhNKrJ_IiTle22fvIi9A4xZJguhlflUySWmGOsadMiDdT-i3q-c-JpGkQfm2DfGZt3kXW_j1gi4IcjIYrXdzZ9bkGRO7roiwpWBMRaZ58gSWSzWzeBkNFN9Tq2gxWPO-Zd-YT9F8NEEHua7DOgSLK3BcdBX7YmUeZ7Vc8nh8fXEAsMjIwztZkN9jUPyPaXCeCpiHrB6gTkIH1H_qy8RZSWYe8d5RM0Tmk5e6t6SsZV87sjTaZJPoZdFFRcdZYJwZVZSsxmVu59SXDV32x2hVQlDWgz3qw32gIxEFOVPg5nHoEzyVfxyGi0# Article: file:///Users/pavely/Downloads/raft.pdf

import collections

from raft_timers import RandomizedTimer

from raft_leader import Leader

from raft_protocol import AppendEntriesRequest, AppendEntriesResponse, \
    LogEntry, RequestVoteRequest, RequestVoteResponse

from raft_roles import Candidate, Follower, FollowerDelegate

# Imports for test
from raft_leader import ReplicaTracker, BackoffPolicy


ReplicaConfig = collections.namedtuple('ReplicaConfig',
    "min_election_delay, max_election_delay, message_timeout, " +
    "heartbeat_interval")

class ReplicaDelegate:
  def ApplyCommands(self, last_applied_index, commit_index):
    pass
  def LeaderStateChanged(self, is_leader):
    pass


class Replica:
  STATE_FOLLOWER = "Follower"
  STATE_CANDIDATE = "Candidate"
  STATE_LEADER = "Leader"
  def __init__(self, replica_set, replica_id, config,
      persistent_state, network, scheduler, rnd):
    self.replica_set = replica_set
    self.replica_id = replica_id
    self.persistent_state = persistent_state
    self.network = network
    self.scheduler = scheduler
    self.rnd = rnd
    self.config = config
    self.election_timer = RandomizedTimer(
        config.min_election_delay, config.max_election_delay,
        self.rnd.random(), self.scheduler)
    self.election_timer.SetCallback(self.StartElection_)
    self.replica_delegate = None
    self.state = None
    self.follower = None
    self.candidate = None
    self.leader = None
    self.last_applied_index = 0
    self.last_known_leader = None

  def SetReplicaDelegate(self, replica_delegate):
    self.replica_delegate = replica_delegate

  def Start(self):
    assert(self.replica_delegate)
    self.TransitionIntoState_(self.STATE_FOLLOWER)

  def TransitionIntoState_(self, new_state):
    # print(self.scheduler.Now(), self.replica_id, new_state)
    self.follower = None
    self.candidate = None
    if self.leader:
      self.leader.Stop()
    self.leader = None
    self.election_timer.Stop()
    old_state = self.state
    self.state = new_state
    if old_state == self.STATE_LEADER:
      assert(self.state != old_state)
      self.replica_delegate.LeaderStateChanged(False)
    if self.state == self.STATE_FOLLOWER:
      self.follower = Follower(self.persistent_state, self)
      self.election_timer.Reset()
      return
    elif self.state == self.STATE_CANDIDATE:
      self.candidate = Candidate(
          self.replica_set, self.replica_id,  self.persistent_state,
          self.network, self)
      self.election_timer.Reset()
      return
    elif self.state == self.STATE_LEADER:
      self.leader = Leader(
          self.replica_set, self.replica_id, self.network,
          self.persistent_state, self.scheduler,
          self.config.message_timeout, self.config.heartbeat_interval,
          self)
      return
    assert(False)

  def HandleNetworkMessage(self, src, msg):
    # print(self.replica_id, msg)
    if self.persistent_state.GetCurrentTerm() < msg.term:
      self.persistent_state.SetCurrentTerm(msg.term)
      self.TransitionIntoState_(self.STATE_FOLLOWER)
    if self.state == self.STATE_CANDIDATE and \
        isinstance(msg, AppendEntriesRequest) and \
        self.persistent_state.GetCurrentTerm() == msg.term:
      self.TransitionIntoState_(self.STATE_FOLLOWER)

    if self.state == self.STATE_FOLLOWER:
      if isinstance(msg, AppendEntriesRequest):
        resp = self.follower.HandleAppendEntriesRequest(msg)
        self.network.SendMessage(self.replica_id, src, resp)
      elif isinstance(msg, RequestVoteRequest):
        resp = self.follower.HandleRequestVoteRequest(msg)
        self.network.SendMessage(self.replica_id, src, resp)
      return
    elif self.state == self.STATE_CANDIDATE:
      if isinstance(msg, RequestVoteResponse):
        self.candidate.HandleRequestVoteResponse(src, msg)
      return
    elif self.state == self.STATE_LEADER:
      if isinstance(msg, AppendEntriesResponse):
        self.leader.HandleAppendEntriesResponse(src, msg)
      return
    assert(False)

  def UpdateCommitIndex(self, commit_index):
    if self.last_applied_index < commit_index:
      self.replica_delegate.ApplyCommands(self.last_applied_index, commit_index)
      self.last_applied_index = commit_index

  def BecameLeader(self):
    self.last_known_leader = self.replica_id
    self.TransitionIntoState_(self.STATE_LEADER)
    self.replica_delegate.LeaderStateChanged(True)

  def FollowerAcceptedRequest(self, current_leader):
    self.last_known_leader = current_leader
    self.election_timer.Reset()

  def ReplicateCommand(self, cmd):
    assert(self.state == self.STATE_LEADER)
    return self.leader.ReplicateCommand(cmd)

  def StartElection_(self):
    self.TransitionIntoState_(self.STATE_CANDIDATE)
