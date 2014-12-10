import os
import sys

class Event:
	def __init__(self, time, eventType, data):
		self.time = time
		self.data = data
		self.eventType = eventType

	def __eq__(self, other):
		return self.time == other.time

	def __lt__(self, other):
		return self.time == other.time

	def __str__(self):
		return "time : %d , data : %s , event Type : %s" % (self.time, self.data, self.eventType)

	def __cmp__(self, other):
		if self.time <= other.time:
			return -1
		else:
			return 1 
