import random

class Timer(object):
  def __init__(self, scheduler):
    self.scheduler = scheduler
    self.fire_time = -1
  def SetCallback(self, callback):
    self.callback = callback
  def ResetForInterval(self, interval):
    fire_time = self.scheduler.Now() + interval
    if fire_time == self.fire_time:
      return
    self.fire_time = fire_time
    self.scheduler.PostTaskForTime(self.fire_time, self.TimerFired_)
  def Stop(self):
    self.fire_time = -1
  def TimerFired_(self):
    if self.scheduler.Now() == self.fire_time:
      self.Stop()
      self.callback()

class RandomizedTimer(Timer):
  def __init__(self, min_interval, max_interval, seed, scheduler):
    super(RandomizedTimer, self).__init__(scheduler)
    self.random = random.Random(seed)
    self.min_interval = min_interval
    self.max_interval = max_interval

  def Reset(self):
    interval = self.random.randint(self.min_interval, self.max_interval)
    self.ResetForInterval(interval)

