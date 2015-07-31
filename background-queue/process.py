# Rushy Panchal
# background-queue/process.py

from Queue import Queue, Empty
from threading import Timer

class BackgroundQueueProcessor(Queue):
	'''A time-based processor that takes items off of a queue and processes them on a separate thread'''
	def __init__(self, *args, **kwargs):
		interval = kwargs.pop("interval", 5)

		super(BackgroundQueueProcessor, self).__init__(*args, **kwargs)

		self.current = current
		self.interval = interval
		self.thread = TimedThread(target = self.process_queue, interval = interval)

	def run(self):
		'''Run the BackgroundQueueProcessor'''
		self.thread.run()

	def stop(self):
		'''Stop the BackgroundQueueProcessor'''
		self.join()
		self.thread.stop()

	def condition(self):
		'''Conditional execution of the process'''
		raise NotImplementedError("BackgroundQueueProcessor.condition should be overriden in child classes")

	def process_one(self):
		'''Process self.current, the current object in the queue'''
		raise NotImplementedError("BackgroundQueueProcessor.condition should be overriden in child classes")

	def process_queue(self):
		'''Process the queue, one object at a time'''
		if self.current:
			if self.condition():
				success = self.process_one()
				if not success:
					self.put(self.current, False)
				self.current = None
				self.task_done() # need to mark the task as done even if it did not succeed
				return self.process_queue() # run the process again to try to process another value if possible
		else:
			try:
				self.current = self.get(False)
			except Empty:
				self.current = None
				return False

class TimedThread(object):
	'''A wrapper around the threading.Timer class

	Automates restarting threads at every "interval" seconds'''
	def __init__(self, target = None, interval = 5, prerun = False):
		self.target = target if target else lambda: None
		self.interval = interval

		self.shouldRun = False
		if prerun:
			self.target()
		self.thread = self.timer()
	
	def timer(self):
		'''Return a timer with the correct interval and task'''
		return Timer(self.interval, self._task)

	def _task(self):
		'''Run the target, with timed execution'''
		self.target()
		if self.shouldRun:
			self.thread = self.timer()
			self.thread.start()

	def run(self):
		'''Run the TimedThread'''
		self.shouldRun = True
		self.thread.start()

	def stop(self):
		'''Stop the timed thread'''
		self.shouldRun = False
