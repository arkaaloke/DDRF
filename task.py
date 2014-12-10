import os
import sys

class Task:
    statid = 0
    def __init__(self, dur):
        self.duration = dur
        self.taskid = statid

    def __init__(self, mem , cpu , dur):
        self.duration = dur
        self.cpu = cpu
        self.mem = mem

    def setJob(self, job):
        self.job = job

    def __str__(self):
        return "cpu:%.2f,mem:%.2f === jobid : %d , numtasksremaining: (%d,%d) " %(float(self.cpu), float(self.mem), self.job.jobid, len(self.job.tasksRunning) , len(self.job.tasksReady) )
  
