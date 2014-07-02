#!/usr/bin/env python

from datetime import datetime
from itertools import groupby
import json
import numpy as np
import os
import os.path as path
import sys

class Job(object):
	def __init__(self, jid, jobname, submit_time, launch_time, finish_time, map_tasks, reduce_tasks):
		self.jid = jid
		self.jobname = jobname
		self.submit_time = submit_time
		self.launch_time = launch_time
		self.finish_time = finish_time
		self.map_tasks = map_tasks
		self.reduce_tasks = reduce_tasks
	
	def job_time(self):
		return self.finish_time - self.launch_time

	@property
	def submit_time(self):
		return self._submit_time

	@property
	def launch_time(self):
		return self._launch_time

	@property
	def finish_time(self):
		return self._finish_time

	@submit_time.setter
	def submit_time(self, v):
		self._submit_time = int(v) if not v is None else None

	@launch_time.setter
	def launch_time(self, v):
		self._launch_time = int(v) if not v is None else None

	@finish_time.setter
	def finish_time(self, v):
		self._finish_time = int(v) if not v is None else None

	def __str__(self):
		return '{}({}, {}, {}, {}, maps:{}, reduces:{})'.format(self.jid,
				 self.submit_time, self.launch_time, self.finish_time,
				 self.job_time(),
				 sorted([t.num for t in self.map_tasks.values()]),
				 sorted([t.num for t in self.reduce_tasks.values()]))
		
	def get_runtime(self):
		return (self.finish_time - self.launch_time) / 1000.
	
	def get_totalmapwork(self):
		total = 0
		for t in self.map_tasks.values():
			total += t.get_task_runtime()		
		return total
	
	def get_totalshufflework(self):
		total = 0
		for t in self.reduce_tasks.values():
			total += t.get_shuffle_runtime()		
		return total
	
	def get_totalreducework(self):
		total = 0
		for t in self.reduce_tasks.values():
			total += t.get_task_runtime()		
		return total
	
	def get_totalamountwork(self):
		total = 0
		for t in self.map_tasks.values():
			total += t.get_task_runtime()
		for t in self.reduce_tasks.values():
			total += t.get_task_runtime()
		return total
	
	def get_allreduceruntime(self):
		return sorted([int(t.get_task_runtime()) for t in self.reduce_tasks.values()], reverse=True)
	
	def get_mapruntime(self):
		max = 0
		for t in self.map_tasks.values():
			if t.finish_time > max:
				max = t.finish_time
		min = max
		for t in self.map_tasks.values():
			if t.start_time < min:
				min = t.start_time
		return (max - min) / 1000
	
	def get_reducerTotalRunTime(self):
		return self.get_shuffleruntim() + self.get_reduceruntime()
		
	def get_totalruntime(self):
		return self.get_mapruntime() + self.get_reduceTotalRunTime()

	def get_shuffleruntime(self):
		max = 0
		for t in self.reduce_tasks.values():
			if t.shuffle_time > max:
				max = t.shuffle_time
		min = max
		for t in self.reduce_tasks.values():
			if t.start_time < min:
				min = t.start_time
		return (max - min) / 1000
	
	def get_reduceruntime(self):
		max = 0
		for t in self.reduce_tasks.values():
			if t.finish_time > max:
				max = t.finish_time
		min = max
		for t in self.reduce_tasks.values():
			if t.shuffle_time < min:
				min = t.shuffle_time
		return (max - min) / 1000
		
	def get_avg_reduceInputRecord(self):
		reduce_input_rec = []
		for t in self.reduce_tasks.values():
			reduce_input_rec.append(t.reduce_input_rec)
		
		print reduce_input_rec
		print np.sum(reduce_input_rec)
		if not reduce_input_rec:
			return 0
		return np.mean(reduce_input_rec)

	def get_std_reduceInputRecord(self):
		reduce_input_rec = []
		for t in self.reduce_tasks.values():
			reduce_input_rec.append(t.reduce_input_rec)
		if not reduce_input_rec:
			return 0
		return np.std(reduce_input_rec)


class Task(object):
	def __init__(self, tid, start_time, finish_time, counter, reduce_input_rec):
		# task_201304142219_0001_m_000026
		self.tid = tid
		splitted = tid.split('_')
		self.jid = splitted[1] + '_' + splitted[2]
		self.task_type = splitted[3]
		# print self.jid,self.task_type
		self.num = int(splitted[4])
		self.start_time = start_time
		self.finish_time = finish_time
		self.counter = counter
		self.reduce_input_rec = reduce_input_rec
		self.shuffle_time = None
		self.sort_time = None

	@property
	def reduce_input_rec(self):
		return self._reduce_input_rec
	
	@reduce_input_rec.setter
	def reduce_input_rec(self, v):
		self._reduce_input_rec = int(v) if not v is None else None

	@property
	def start_time(self):
		return self._start_time

	@start_time.setter
	def start_time(self, v):
		self._start_time = int(v) if not v is None else None

	@property
	def finish_time(self):
		return self._finish_time
	
	@finish_time.setter
	def finish_time(self, v):
		self._finish_time = int(v) if not v is None else None

	@property
	def shuffle_time(self):
		return self._shuffle_time
	
	@shuffle_time.setter
	def shuffle_time(self, v):
		self._shuffle_time = int(v) if not v is None else None
		
	@property
	def sort_time(self):
		return self._shuffle_time
	
	@sort_time.setter
	def sort_time(self, v):
		self._shuffle_time = int(v) if not v is None else None

	@property
	def counter(self):
		return self._counter
	
	@counter.setter
	def counter(self, v):
		self._counter = v
		
	def __hash__(self):
		return hash(self.tid)

	def __str__(self):
		return '{}({}, {})'.format(self.tid, self.start_time, self.finish_time)

	def get_task_runtime(self):
		return (self.finish_time - self.start_time) / 1000.
	
	def get_shuffle_runtime(self):
		return (self.shuffle_time - self.start_time) / 1000. if self.shuffle_time != None else None
	
	

def job2dict(job):

	return {job.jid : { 'jobname' : job.jobname,
						'submit_time' : job.submit_time,
						'launch_time' : job.launch_time,
						'finish_time' : job.finish_time,
						'map_tasks' :	tasks2dicts(job.map_tasks),
						'reduce_tasks' : tasks2dicts(job.reduce_tasks) }}

def tasks2dicts(tasks):
	new_tasks = {}
	for tid, task in tasks.items():
		new_tasks[tid] = task2dict(task)
	return new_tasks

def task2dict(task):
	return { 'task_type' : 'map' if task.task_type == 'm' else 'reduce',
			 'start_time' : task.start_time,
			 'finish_time' : task.finish_time,
			 'reduce_input_rec': task.reduce_input_rec}

def unixtime_to_str(t):
	d = datetime.fromtimestamp(t / 1000.) 
	return d.strftime('%H:%M:%S')
	# return d

def remove_str_bounds(s):
	if s[0] == '"' and s[-1] == '"':
		return s[1:-1]
	elif s[0] == '"':
		return s[1:]
	elif s[-1] == '"':
		return s[:-1]
	else:
		return s
	
def parse(events_strings):
	job = Job(None, None, None, None, None, dict(), dict())
	maps, reduces = job.map_tasks, job.reduce_tasks
	for event_string in events_strings:
		splitted = event_string.split()
		if not splitted:
			continue
		if splitted[0] == 'Job':
			parse_job_event(splitted[1:], job)
		elif splitted[0] == 'Task':
			parse_task_event(splitted[1:], maps, reduces)
		elif splitted[0] in ['ReduceAttempt', 'MapAttempt']:
			parse_attempt_event(splitted[1:], maps, reduces)
			
	def filter_defined(d):
		for (e, v) in d.items():
			if not (v.start_time and v.finish_time):
				print '{}.{} not defined, ignoring it'.format(e,
							 'start_time' if not v.start_time else 'finish_time')
				del d[e]
				
	filter_defined(maps)
	filter_defined(reduces)
	return job

def parse_job_event(event, job):
	event_dict = build_event_dict(event)
	job.jid = event_dict['JOBID']
	if 'JOBNAME' in event_dict:
		job.name = event_dict['JOBNAME']
	if 'SUBMIT_TIME' in event_dict:
		job.submit_time = event_dict['SUBMIT_TIME']
	if'LAUNCH_TIME' in event_dict:
		job.launch_time = event_dict['LAUNCH_TIME']
	if 'FINISH_TIME' in event_dict:
		job.finish_time = event_dict['FINISH_TIME']
	

def parse_task_event(event, maps, reduces):
# 		print splitted, map(len,splitted)
	event_dict = build_event_dict(event)
	task_type = event_dict['TASK_TYPE']
	if task_type != 'MAP' and task_type != 'REDUCE':
		return
# 	print event_dict
	tid = event_dict['TASKID']
	is_map = task_type == 'MAP'
# 	print tid,task_type,is_map
	tasks = maps if is_map else reduces
	is_new = not tid in tasks
	task = Task(tid, None, None, None, None) if is_new else tasks[tid]
	if 'START_TIME' in event_dict:
		task.start_time = int(event_dict['START_TIME'])
	elif 'FINISH_TIME' in event_dict:
		task.finish_time = int(event_dict['FINISH_TIME'])
	elif 'COUNTERS' in event_dict:
		task.finish_time = int(event_dict['COUNTERS'])
	elif 'REDUCE_INPUT_RECORDS' in event_dict:
		task.reduce_input_rec = int(event_dict['REDUCE_INPUT_RECORDS'])
	
	if is_new:
		tasks[tid] = task
	return maps, reduces

def parse_attempt_event(event, maps, reduces):
	event_dict = build_event_dict(event)

	task_type = event_dict['TASK_TYPE']
	if task_type != 'MAP' and task_type != 'REDUCE':
		return
	
	tasks = maps if task_type == 'MAP' else reduces
	
	status = event_dict.get('TASK_STATUS')

	rec_input_rec = event_dict.get('REDUCE_INPUT_RECORDS')	

	if status == "KILLED":
		return
	
	tid = event_dict['TASKID']
	if not tid in tasks:
		print "Error TaskID not in task list. This shouldn't happen"
		return
	
	task = tasks[tid]

	if 'START_TIME' in event_dict:
		task.start_time = int(event_dict['START_TIME'])
	elif 'SHUFFLE_FINISHED' in event_dict:
		task.shuffle_time = int(event_dict['SHUFFLE_FINISHED'])
	elif 'SORT_FINISHED' in event_dict:
		task.sort_time = int(event_dict['SORT_FINISHED'])
	elif 'FINISH_TIME' in event_dict:
		task.finish_time = int(event_dict['FINISH_TIME'])
	
	if task_type == 'REDUCE':
		task.reduce_input_rec = rec_input_rec

	return reduces


def build_event_dict(event):
	event_dict = {}
	for keyval in event[:-1]:
		keyval_splitted = keyval.split('=')
		if len(keyval_splitted) < 2:
			continue
		event_dict[remove_str_bounds(keyval_splitted[0])] = \
			remove_str_bounds(keyval_splitted[1])
	
	for keyval in event[:-1]:
		keyval_splitted = keyval.split('][(REDUCE_OUTPUT_RECORDS)(Reduce')
		if len(keyval_splitted) < 2:
			continue
		keyval_splitted2 = keyval_splitted[0].split(')(')
		keyval_splitted3 = keyval_splitted2[1].split(')')
		event_dict["REDUCE_INPUT_RECORDS"] = keyval_splitted3[0]

	return event_dict

def flatzip(xs, ys):
	""" flatzip('ABC', 'DEF') => 'ADBECF' """
	return [i for z in zip(xs, ys) for i in z]

def main():
	if len(sys.argv) < 2:
		sys.exit("Usage: {} <job_history_files>".format(sys.argv[0]))
	for i in range(1, len(sys.argv)):
		with open(sys.argv[i], 'rt') as fp:
			lines = fp.read().split("\n")
		job = parse(lines)
		# print sys.argv[i] + "\t" + str(int(job.get_runtime())) + "\t" + str(int(job.get_totalmapwork())) + "\t" + str(int(job.get_totalshufflework())) + "\t" + str(int(job.get_totalreducework()))
		# print sys.argv[i] + "\t" + str(job.get_allreduceruntime())
		#print sys.argv[i] + "\t" + str(job.get_mapruntime()) + "\t" + str(job.get_reducerTotalRunTime()) + "\t" + str(job.get_totalruntime())
		print sys.argv[i] + "\t" + str(job.get_avg_reduceInputRecord()) + "\t" + str(job.get_std_reduceInputRecord())
	

if __name__ == '__main__':
	main()
