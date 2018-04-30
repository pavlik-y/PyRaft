import random

import components
import raft

class App:
  def __init__(self, replica, persistent_state):
    self.replica = replica
    self.replica.SetReplicaDelegate(self)
    self.persistent_state = persistent_state
    self.x = 0
  def ApplyCommands(self, last_applied_index, commit_index):
    print("Apply commands:%s:%s" % (last_applied_index, commit_index))
    for index in xrange(last_applied_index + 1, commit_index + 1):
      log_entry = self.persistent_state.GetLogEntry(index)
      print(index, log_entry)
      self.x += log_entry.cmd
      print("STATE:%s:x=%d" %(self.replica.replica_id, self.x))

  def LeaderStateChanged(self, is_leader):
    pass
  def ReplicateCommand(self, command):
    self.replica.ReplicateCommand(command)

  def GetLastKnownLeader(self):
    return self.replica.last_known_leader

class Env:
  def __init__(self, replica_set, seed):
    self.replica_set = replica_set
    self.message_queue = components.MessageQueue()
    self.network = components.Network(self.message_queue)
    self.storages = {}
    self.replicas = {}
    self.apps = {}
    self.random = random.Random(seed)
  def CreateStorage(self, replica_id):
    assert(replica_id not in self.storages)
    self.storages[replica_id] = components.PersistentState()
  def DeleteStorage(self, replica_id):
    del self.storages[replica_id]
  def LeaderStateChanged(self, leader):
    print("LeaderStateChanged")
  def CreateReplica(self, replica_id):
    persistent_state = self.storages[replica_id]
    replica_config = raft.ReplicaConfig(
        min_election_delay=500, max_election_delay=900, message_timeout=10,
        heartbeat_interval=100)
    replica = raft.Replica(self.replica_set, replica_id, replica_config,
        persistent_state, self.network, self.message_queue,
        random.Random(self.random.random()))
    self.network.RegisterReplica(replica_id, replica)
    self.replicas[replica_id] = replica
    app = App(replica, persistent_state)
    self.apps[replica_id] = app
    replica.SetReplicaDelegate(app)
    replica.Start()
  def DeleteReplica(self, replica_id):
    self.network.UnregisterReplica(replica_id)
    del self.replicas[replica_id]
    del self.apps[replica_id]

  def ReplicateCommand(self, command):
    replica_id = self.random.choice(list(self.replica_set))
    app = self.apps[replica_id]
    while app.GetLastKnownLeader() != None and \
          app.GetLastKnownLeader() != replica_id:
      replica_id = app.GetLastKnownLeader()
      app = self.apps[replica_id]
    if app.GetLastKnownLeader() == None:
      print("No current leader")
    else:
      app.ReplicateCommand(command)

class NetworkDeliveryPolicy:
  def __init__(self):
    self.index = 0
  def DelayedMessageDelivery(self, network, src, dst, msg):
    self.index += 1
    if self.index % 3 == 0:
      print("Dropping message:%s=>%s:%s" % (src, dst, msg))
      return
    network.ScheduleDelayedMessageDelivery(50, src, dst, msg)

def DisconnectR2(network, src, dst, msg):
  if src == 'r2' or dst == 'r2':
    # print("Blocked message:%s=>%s:%s" %(src, dst, msg))
    return
  network.ScheduleDelayedMessageDelivery(1, src, dst, msg)

def main():
  replica_set = {'r1', 'r2', 'r3'}
  env = Env(replica_set, 2208)
  # network_policy = NetworkDeliveryPolicy()
  # env.network.SetDeliveryPolicy(network_policy.DelayedMessageDelivery)
  env.network.SetDeliveryPolicy(DisconnectR2)

  for id in replica_set:
    env.CreateStorage(id)
    env.CreateReplica(id)
  # At random intervals client comes in, chooses random replica, tries to find
  # leader and tries to replicate random command
  env.message_queue.PostDelayedTask(1000, lambda: env.ReplicateCommand(10))
  env.message_queue.PostDelayedTask(2000, lambda: env.ReplicateCommand(-20))
  env.message_queue.PostDelayedTask(3000, lambda: env.ReplicateCommand(30))
  env.message_queue.PostDelayedTask(4000,
      lambda: env.network.SetDeliveryPolicy(
          env.network.PolicyDeliverMessageImmediately))

  # for _ in xrange(3000):
  env.message_queue.RunUntilTime(6000)
  for id in replica_set:
    env.DeleteReplica(id)
    env.DeleteStorage(id)



if __name__ == '__main__':
  main()