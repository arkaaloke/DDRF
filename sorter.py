import os
import sys


class DRFSorter:
		
	def sort(self, jobs):
		jobs = sorted(jobs, key=lambda k:k.getDomShare())
	
