import collections
import random
import unittest

import components
import raft

def MakeAppendEntriesResponse(term, success, match_index, request):
  return raft.AppendEntriesResponse(term, success, match_index,
      request.message_id)

class MockCall:
  def __init__(self, delegate):
    self.delegate = delegate
    self.call_count = 0

  def __call__(self, *args, **kvargs):
    self.call_count += 1
    self.last_args = args
    self.last_kvargs = kvargs
    return self.delegate(*args, **kvargs)
  def CalledWithParams(self, *args, **kvargs):
    if self.call_count == 0:
      return False
    return self.last_args == args and self.last_kvargs == kvargs

class RecordingNetwork:
  def __init__(self):
    self.ClearMessages()
  def SendMessage(self, src, dst, msg):
    self.messages[dst].append(msg)
  def ClearMessages(self):
    self.messages = collections.defaultdict(list)

class PersistenStateTest(unittest.TestCase):
  def testSetCurrentTerm(self):
    state = components.PersistentState()
    state.SetCurrentTerm(1)
    self.assertEquals(state.GetCurrentTerm(), 1);
    state.SetVotedFor(1, "replica1")
    self.assertEquals(state.GetVotedFor(), "replica1");
    state.SetCurrentTerm(2)
    self.assertEquals(state.GetCurrentTerm(), 2);
    self.assertEquals(state.GetVotedFor(), None);
    index, term = state.GetLastLogIndexAndTerm()
    self.assertEquals(0, index)
    self.assertEquals(0, term)
    state.AppendLogEntry(raft.LogEntry(1, 2, None))
    index, term = state.GetLastLogIndexAndTerm()
    self.assertEquals(1, index)
    self.assertEquals(2, term)
    state.DeleteLogEntriesAfterIndex(0)
    index, term = state.GetLastLogIndexAndTerm()
    self.assertEquals(0, index)
    self.assertEquals(0, term)

class MessageQueueTest(unittest.TestCase):
  def setUp(self):
    self.call_count = 0

  def Callback(self):
    self.call_count += 1

  def testMessageQueue(self):
    mq = components.MessageQueue()
    self.assertEqual(0, mq.Now())
    mq.PostDelayedTask(20, self.Callback)
    mq.PostDelayedTask(10, self.Callback)
    mq.RunOneTask()
    self.assertEqual(10, mq.Now())
    self.assertEqual(1, self.call_count)
    mq.RunOneTask()
    self.assertEqual(20, mq.Now())
    self.assertEqual(2, self.call_count)
    mq.RunUntilIdle()

class NetworkTest(unittest.TestCase):
  def setUp(self):
    self.message_delivered = False
  def HandleNetworkMessage(self, src, msg):
    self.message_delivered = True
  def testNetwork(self):
    mq = components.MessageQueue()
    network = components.Network(mq)
    network.RegisterReplica('r1', self)
    network.SendMessage('r2', 'r1', 'cmd')
    self.assertFalse(self.message_delivered)
    mq.RunUntilIdle()
    self.assertTrue(self.message_delivered)

class TimerTest(unittest.TestCase):
  def setUp(self):
    self.message_queue = components.MessageQueue()
    self.timer_fired = False
  def TimerFired(self):
    self.timer_fired = True
  def testTimer(self):
    timer = raft.RandomizedTimer(10, 10, 42, self.message_queue)
    timer.SetCallback(self.TimerFired)
    timer.Reset()
    self.message_queue.PostDelayedTask(5, lambda: timer.Reset())
    self.message_queue.RunUntilIdle()
    self.assertEqual(15, self.message_queue.Now())
    self.assertTrue(self.timer_fired)
  def testTimerStopped(self):
    timer = raft.RandomizedTimer(10, 10, 42, self.message_queue)
    timer.SetCallback(self.TimerFired)
    timer.Reset()
    timer.Stop()
    self.message_queue.RunUntilIdle()
    self.assertFalse(self.timer_fired)

class FollowerTest(unittest.TestCase):
  def setUp(self):
    state = components.PersistentState()
    self.state = state
    state.SetCurrentTerm = MockCall(state.SetCurrentTerm)
    state.SetVotedFor = MockCall(state.SetVotedFor)
    state.AppendLogEntry = MockCall(state.AppendLogEntry)
    state.DeleteLogEntriesAfterIndex = MockCall(state.DeleteLogEntriesAfterIndex)
    self.replica_delegate = raft.FollowerDelegate()
    self.replica_delegate.UpdateCommitIndex = MockCall(
        self.replica_delegate.UpdateCommitIndex)
    self.replica_delegate.FollowerAcceptedRequest = MockCall(
        self.replica_delegate.FollowerAcceptedRequest)

  def testRequestVote(self):
    self.state.SetCurrentTerm(2)
    self.state.SetCurrentTerm.call_count = 0

    follower = raft.Follower(self.state, self.replica_delegate)

    # Vote for past term
    response = follower.HandleRequestVoteRequest(
        raft.RequestVoteRequest(1, "r1", 0, 0))
    self.assertEquals(
        response, raft.RequestVoteResponse(2, False))
    self.assertEquals(self.state.GetCurrentTerm(), 2)
    self.assertEquals(self.state.GetVotedFor(), None)
    self.assertTrue(self.state.SetCurrentTerm.call_count == 0)
    self.assertTrue(self.state.SetVotedFor.call_count == 0)
    self.assertEqual(0,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Vote accepted
    response = follower.HandleRequestVoteRequest(
        raft.RequestVoteRequest(2, "r1", 0, 0))
    self.assertEquals(
        response, raft.RequestVoteResponse(2, True))
    self.assertEquals(self.state.GetVotedFor(), "r1")
    self.assertTrue(self.state.SetCurrentTerm.call_count == 0)
    self.assertTrue(self.state.SetVotedFor.call_count == 1)
    self.assertEqual(1,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Vote conflicts with VotedFor
    response = follower.HandleRequestVoteRequest(
        raft.RequestVoteRequest(2, "r2", 0, 0))
    self.assertEquals(
        response, raft.RequestVoteResponse(2, False))
    self.assertEquals(self.state.GetVotedFor(), "r1")
    self.assertTrue(self.state.SetCurrentTerm.call_count == 0)
    self.assertEqual(1,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

  def testAppendEntries(self):
    self.state.SetCurrentTerm(2)
    follower = raft.Follower(self.state, self.replica_delegate)

    # Append entries for previous term
    response = follower.HandleAppendEntriesRequest(
        raft.AppendEntriesRequest(1, "r1", 5, 1, [], 5, "msg_1"))
    self.assertEquals(
        response, raft.AppendEntriesResponse(2, False, 5, "msg_1"))
    self.assertEquals(self.state.GetCurrentTerm(), 2)
    self.assertEquals(self.state.DeleteLogEntriesAfterIndex.call_count, 0)
    self.assertEqual(0,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Append entries, current index, empty new entries (heartbeat)
    response = follower.HandleAppendEntriesRequest(
        raft.AppendEntriesRequest(2, "r1", 0, 0, [], 0, "msg_2"))
    self.assertEquals(
        response, raft.AppendEntriesResponse(2, True, 0, None))
    self.assertEquals(self.state.AppendLogEntry.call_count, 0)
    self.assertEqual(1,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Append new entry, no delete
    response = follower.HandleAppendEntriesRequest(
        raft.AppendEntriesRequest(2, "r1", 0, 0, [raft.LogEntry(1, 2, None)],
            0, "msg_3"))
    self.assertTrue(self.state.GetLogEntry(1) != None)
    self.assertTrue(self.state.AppendLogEntry.call_count == 1)
    self.assertEqual(2,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Append new entry with delete
    response = follower.HandleAppendEntriesRequest(
        raft.AppendEntriesRequest(2, "r1", 0, 0, [raft.LogEntry(1, 3, None)],
            0, "msg_4"))
    self.assertTrue(self.state.GetLogEntry(1) != None)
    self.assertTrue(self.state.AppendLogEntry.call_count == 2)
    self.assertEqual(3,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Advance leader_commit_index
    response = follower.HandleAppendEntriesRequest(
        raft.AppendEntriesRequest(2, "r1", 1, 3, [], 1, "msg_5"))
    self.assertTrue(self.state.GetLogEntry(1) != None)
    self.assertTrue(self.state.AppendLogEntry.call_count == 2)
    self.assertEqual(4,
        self.replica_delegate.FollowerAcceptedRequest.call_count)

    # Verify delegate.EntriesCommitted was called
    self.assertEqual(1, self.replica_delegate.UpdateCommitIndex.call_count)

class CandidateTest(unittest.TestCase):
  def setUp(self):
    self.persistent_state = components.PersistentState()
    self.network = RecordingNetwork()
    self.replica_set = {'r1', 'r2', 'r3'}
    self.replica_id = 'r1'
    self.BecameLeader = MockCall(
        self.BecameLeader)

  def BecameLeader(self):
    pass

  def testCandidate(self):
    candidate = raft.Candidate(self.replica_set, 'r1', self.persistent_state,
        self.network, self)
    self.assertListEqual(self.network.messages['r2'], [raft.RequestVoteRequest(1, 'r1', 0, 0)])
    self.assertListEqual(self.network.messages['r3'], [raft.RequestVoteRequest(1, 'r1', 0, 0)])
    self.assertEqual(1, self.persistent_state.GetCurrentTerm())

    candidate.HandleRequestVoteResponse('r2',
        raft.RequestVoteResponse(1, True))
    self.assertEqual(1, self.BecameLeader.call_count)

class ReplicaTrackerTest(unittest.TestCase):
  Message = collections.namedtuple('Message', "success, index, message_id")
  @classmethod
  def Request(cls, id):
    return cls.Message(True, 0, id)
  def testReplicaTracker(self):
    backoff_policy = raft.BackoffPolicy(10)
    replica_tracker = raft.ReplicaTracker("r1", 100, backoff_policy, 100)
    self.assertEqual(replica_tracker.GetMessageToSend(1000), (100, 100))
    replica_tracker.MessageSent(self.Request("id1"), 1000)
    self.assertEqual(replica_tracker.GetMessageToSend(1000), None)
    self.assertEqual(replica_tracker.GetDelayUntilNextNudge(1000), 10)
    self.assertEqual(replica_tracker.GetMessageToSend(1009), None)
    self.assertEqual(replica_tracker.GetDelayUntilNextNudge(1009), 1)
    self.assertEqual(replica_tracker.GetMessageToSend(1010), (100, 100))
    replica_tracker.ResponseReceived(self.Message(False, 100, "id1"), 1001)
    self.assertEqual(replica_tracker.GetMessageToSend(1001), (99, 99))
    replica_tracker.MessageSent(self.Request("id1"), 1001)
    replica_tracker.ResponseReceived(self.Message(True, 99, "id1"), 1002)
    self.assertEqual(replica_tracker.GetMessageToSend(1010), (99, 100))
    replica_tracker.LastIndexUpdated(110, 1010)
    self.assertEqual(replica_tracker.GetMessageToSend(1010), (99, 110))
    replica_tracker.MessageSent(self.Request("id1"), 1010)
    self.assertEqual(replica_tracker.GetDelayUntilNextNudge(1010), 10)
    replica_tracker.ResponseReceived(self.Message(True, 110, "id1"), 1020)
    self.assertEqual(replica_tracker.GetMessageToSend(1020), None)
    self.assertEqual(replica_tracker.GetDelayUntilNextNudge(1020), 90)

class LeaderTest(unittest.TestCase):
  def setUp(self):
    self.replica_set = {'r1', 'r2', 'r3'}
    self.replica_id = 'r1'
    self.persistent_state = components.PersistentState()
    self.persistent_state.SetCurrentTerm(1)
    self.persistent_state.SetVotedFor(1, 'r1')
    self.network = RecordingNetwork()
    self.scheduler = components.MessageQueue()
    self.UpdateCommitIndex = MockCall(
        self.UpdateCommitIndex)

  def UpdateCommitIndex(self, commit_index):
    pass

  def CreateLeader(self):
    leader = raft.Leader(
        self.replica_set, self.replica_id, self.network, self.persistent_state,
        self.scheduler, 10, 100, self)
    return leader

  def testHeartbeat(self):
    leader = self.CreateLeader()
    self.assertEqual(self.persistent_state.GetLastLogIndexAndTerm(),
                     (0, 0))
    self.assertListEqual(self.network.messages['r2'],
        [raft.AppendEntriesRequest(1, 'r1', 0, 0, [], 0, None)])
    r2_message = self.network.messages['r2'][0]
    self.assertListEqual(self.network.messages['r3'],
        [raft.AppendEntriesRequest(1, 'r1', 0, 0, [], 0, None)])
    r3_message = self.network.messages['r3'][0]
    self.assertListEqual(self.network.messages['r1'], [])
    self.network.ClearMessages()

    self.assertEqual(self.UpdateCommitIndex.call_count, 0)

    leader.HandleAppendEntriesResponse('r2',
        MakeAppendEntriesResponse(1, True, 0, r2_message))
    self.assertEqual(self.UpdateCommitIndex.call_count, 0)
    now = self.scheduler.Now()
    self.scheduler.RunUntilTime(now + 100)

    self.assertEqual(self.persistent_state.GetLastLogIndexAndTerm(),
                     (0, 0))
    self.assertListEqual(self.network.messages['r2'],
        [raft.AppendEntriesRequest(1, 'r1', 0, 0, [], 0, None)])
    self.assertListEqual(self.network.messages['r1'], [])

    leader.HandleAppendEntriesResponse('r3',
        MakeAppendEntriesResponse(1, True, 0, r3_message))

  def testReplicateCommand(self):
    leader = self.CreateLeader()
    self.assertEqual(self.persistent_state.GetLastLogIndexAndTerm(),
                     (0, 0))
    self.assertListEqual(self.network.messages['r2'],
        [raft.AppendEntriesRequest(1, 'r1', 0, 0, [], 0, None)])
    leader.HandleAppendEntriesResponse('r2',
        MakeAppendEntriesResponse(1, True, 0, self.network.messages['r2'][0]))
    self.assertListEqual(self.network.messages['r3'],
        [raft.AppendEntriesRequest(1, 'r1', 0, 0, [], 0, None)])
    leader.HandleAppendEntriesResponse('r3',
        MakeAppendEntriesResponse(1, True, 0, self.network.messages['r3'][0]))
    self.network.ClearMessages()

    leader.ReplicateCommand("cmd")
    self.assertEqual(self.persistent_state.GetLogEntry(1),
        raft.LogEntry(1, 1, "cmd"))
    self.assertEqual(self.UpdateCommitIndex.call_count, 0)
    leader.HandleAppendEntriesResponse('r2',
        MakeAppendEntriesResponse(1, True, 1, self.network.messages['r2'][0]))
    self.assertEqual(self.UpdateCommitIndex.call_count, 1)

class ReplicaTest(unittest.TestCase):
  def setUp(self):
    self.replica_set = {'r1', 'r2', 'r3'}
    self.replica_id = 'r1'
    self.persistent_state = components.PersistentState()
    self.network = RecordingNetwork()
    self.scheduler = components.MessageQueue()

    self.last_applied_index = 0
    self.leader_state = False

  def CreateReplica(self):
    replica = raft.Replica(self.replica_set, self.replica_id,
        raft.ReplicaConfig(min_election_delay=100, max_election_delay=100,
            message_timeout=10, heartbeat_interval=100), self.persistent_state, self.network,
        self.scheduler, random.Random(42))
    # replica.SetRoleFactoryForTest(self)
    replica.SetReplicaDelegate(self)
    replica.Start()
    return replica

  def ApplyCommands(self, last_aplied_index, commit_index):
    self.assertEqual(self.last_applied_index, last_aplied_index)
    self.last_applied_index = commit_index

  def LeaderStateChanged(self, leader_state):
    self.leader_state = leader_state

  def testAcceptEntries(self):
    replica = self.CreateReplica()

    replica.HandleNetworkMessage('r2', raft.AppendEntriesRequest(
        1, 'r2', 0, 0, [raft.LogEntry(1, 1, "cmd1")], 0, "msg_1"))
    self.assertEqual(self.persistent_state.GetLastLogIndexAndTerm(), (1, 1))
    self.assertEqual(0, self.last_applied_index)
    self.assertListEqual(self.network.messages['r2'],
      [raft.AppendEntriesResponse(1, True, 1, "msg_1")])
    replica.HandleNetworkMessage('r2', raft.AppendEntriesRequest(
        1, 'r2', 1, 1, [], 1, "msg_2"))
    self.assertEqual(1, self.last_applied_index)

    replica.HandleNetworkMessage('r2', raft.AppendEntriesRequest(
        1, 'r2', 1, 1, [raft.LogEntry(2, 1, "cmd2")], 1, "msg_3"))
    self.assertEqual(1, self.last_applied_index)

  def testElection(self):
    replica = self.CreateReplica()
    self.scheduler.RunUntilTime(100)
    self.assertEqual(raft.Replica.STATE_CANDIDATE, replica.state)
    self.assertListEqual(self.network.messages['r2'],
        [raft.RequestVoteRequest(1, 'r1', 0, 0)])
    self.assertListEqual(self.network.messages['r3'],
        [raft.RequestVoteRequest(1, 'r1', 0, 0)])
    self.network.ClearMessages()
    self.scheduler.RunUntilTime(200)
    self.assertEqual(raft.Replica.STATE_CANDIDATE, replica.state)
    self.assertListEqual(self.network.messages['r2'],
        [raft.RequestVoteRequest(2, 'r1', 0, 0)])
    self.assertListEqual(self.network.messages['r3'],
        [raft.RequestVoteRequest(2, 'r1', 0, 0)])
    self.assertFalse(self.leader_state)

    replica.HandleNetworkMessage('r2', raft.RequestVoteResponse(2, True))
    self.assertEqual(raft.Replica.STATE_LEADER, replica.state)
    self.assertTrue(self.leader_state)

  def testReplication(self):
    replica = self.CreateReplica()
    self.scheduler.RunUntilTime(100)
    replica.HandleNetworkMessage('r2', raft.RequestVoteResponse(1, True))
    self.assertTrue(self.leader_state)
    index = replica.ReplicateCommand("cmd1")
    self.assertEqual(1, index)


if __name__ == '__main__':
  unittest.main()