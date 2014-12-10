import os
import sys
from Queue import *
from jobqueue import *

class DRFQueue(JobQueue):

	def getJob(self):
		if len(self.jobs) == 0:
			return None
		self.sched.sort(self.jobs)

		#print "NUMBER OF JOBS : ", len(self.jobs)
		#print "SORTED ORDER :"
		#for i in range(len(self.jobs)):
		#	print self.jobs[i]
		j = self.jobs[0]
		return j

