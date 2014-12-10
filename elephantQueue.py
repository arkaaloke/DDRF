import os
import sys
from Queue import *
from jobqueue import *

class ElephantQueue(JobQueue):
	def getJob(self):
		j = self.jobs[0]
		return j

	def getJobById(self, jobid):
		for j in self.jobs:
			if j.jobid == jobid:
				return j

		for j in self.fullyRunningJobs:
			if j.jobid == jobid:
				return j
		return None
