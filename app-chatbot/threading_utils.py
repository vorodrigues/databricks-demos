from threading import Thread

TIMEOUT_SECONDS = 60

class NextThread(Thread):

  def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, arg=None):
    Thread.__init__(self, group=group, target=target, name=name, args=args, kwargs=kwargs)
    self.target = target
    self.arg = arg
    self.result = None
    self.exception = None

  def run(self):
    try:
      self.result = self.target(self.arg)
    except StopIteration as e:
      self.exception = e

  def join(self, timeout=TIMEOUT_SECONDS):
    Thread.join(self, timeout)
    if self.exception:
      raise self.exception
    if self.is_alive():
      raise TimeoutError