import os
import sys
import time
import subprocess
import yaml
import logging
import warnings

import numpy as np
import json
import zmq
import cortex
import pyo

from matplotlib import pyplot as plt
plt.ion()

from .utils import get_database_directory, get_recording_directory, get_configuration_directory, generate_command
config_dir = get_configuration_directory()
db_dir = get_database_directory()
rec_dir = get_recording_directory()

logger = logging.getLogger('stimulate.ion')

class Stimulator(object):
	def __init__(self, stim_config):
		zmq_context = zmq.Context()
		self.input_socket = zmq_context.socket(zmq.SUB)
		self.input_socket.connect('tcp://localhost:5557')
		self.input_socket.setsockopt(zmq.SUBSCRIBE, '')
		self.active = False

		with open(os.path.join(config_dir, stim_config+'.conf'), 'r') as f:
			config = yaml.load(f)
			self.initialization = config.get('initialization', dict())
			self.pipeline = config['pipeline']
			self.global_defaults = config.get('global_defaults', dict())
		
		if self.global_defaults['recording_id'] is None:
			self.global_defaults['recording_id'] = '%s_%s'%(self.global_defaults['subject'], time.strftime('%Y%m%d_%H%M'))
		try:
			os.mkdir(os.path.join(rec_dir, self.global_defaults['recording_id']))
			logger.info('making recording directory for id %s' % self.global_defaults['recording_id'])
		except OSError:
			warnings.warn('Recording id %s already exists!' % self.global_defaults['recording_id'])

		for init in self.initialization:
			logger.debug('initializing %s' % init['name'])
			params = init.get('kwargs', {})	
			for k,v in self.global_defaults.iteritems():
				params.setdefault(k, v)
			init['instance'].__init__(**params)
		for step in self.pipeline:
			logger.debug('initializing %s' % step['name'])
			params = step.get('kwargs', {})
			for k,v in self.global_defaults.iteritems():
				params.setdefault(k, v)
			step['instance'].__init__(**params)

	def run(self):
		self.active = True
		logger.info('running')
		for init in self.initialization:
			logger.debug('starting %s' % init['name'])
			init['instance'].run()

		while self.active:
			try:
				logger.debug('start receive wait')
				msg = self.input_socket.recv()
				logger.debug('received message')
				topic_end = msg.find(' ')
				topic = msg[:topic_end]
				data = msg[topic_end+1:]
				for stim in self.pipeline:
					if topic in stim['topic'].keys():
						logger.debug('sending data of length %i to %s'%(len(data), topic))
						stim['instance'].run({stim['topic'][topic]: data})
						logger.debug('%s function returned'%stim['name'])
			except (KeyboardInterrupt, SystemExit):
				self.active = False
				for init in self.initialization:
					init['instance'].stop()
				for stim in self.pipeline:
					stim['instance'].stop()
				sys.exit(0)

class Stimulus(object):
	def __init__(self, **kwargs):
		self.subject = kwargs.get('subject', None)
		self.record = kwargs.get('record', False)
		self.recording_id = kwargs.get('recording_id', None)
	def stop(self):
		logger.info('stopping %s' % type(self))
	def run(self):
		raise NotImplementedError

class PyCortexViewer(Stimulus):
	def __init__(self, vmin=-1., vmax=1., **kwargs):
		super(PyCortexViewer, self).__init__(**kwargs)
		subject = kwargs.get('subject')
		xfm_name = kwargs.get('xfm_name')
		mask_type = kwargs.get('mask_type')
		npts = cortex.db.get_mask(subject, xfm_name, mask_type).sum()
		
		data = np.zeros(npts)
		vol = cortex.Volume(data, subject, xfm_name)

		self.subject = subject
		self.xfm_name = xfm_name
		self.mask_type = mask_type

		self.ctx_client = cortex.webshow(vol)
		self.vmin = vmin
		self.vmax = vmax
		self.active = True
		logger.debug('initialized PyCortexViewer')

	def run(self, inp):
		if self.active:
			try:
				data = np.fromstring(inp['data'], dtype=np.float32)
				vol = cortex.Volume(data, self.subject, self.xfm_name, vmin=self.vmin, vmax=self.vmax)
				self.ctx_client.addData(data=vol)
			except IndexError:
				self.active = False

class AudioRecorder(Stimulus):
	def __init__(self, **kwargs):
		super(AudioRecorder, self).__init__(**kwargs)
		jack_port = kwargs['jack_port']
		name = kwargs['name']
		path = os.path.join(rec_dir, self.recording_id, name+'.wav')
		try:
			os.makedirs(os.path.dirname(path))
		except OSError:
			pass
		params = [
			{'name': 'out_path', 'flag': 'f', 'value': path},
			{'name': 'duration', 'flag': 'd', 'value': '-1'},
			{'name': 'jack_port', 'position': 'last', 'value': jack_port}
		]
		self.cmd = generate_command('jack_rec', params)
		self.path = path
	def run(self):
		self.proc = subprocess.Popen(self.cmd)
	def stop(self):
		logger.debug('stopping AudioRecorder')
		self.proc.terminate()
		# params = [
		# 	{'name': 'input', 'position': 'first', 'value': self.path},
		# 	{'name': 'output', 'position': 'last', 'value': self.path.replace('.wav', '.mp3')}
		# ]
		# cmd = generate_command('sox', params)
		# subprocess.call(cmd)

class ConsolePlot(Stimulus):
	def __init__(self, xmin=-2., xmax=2., width=40, **kwargs):
		super(ConsolePlot, self).__init__()
		self.xmin = xmin
		self.xmax = xmax
		self.x_range = xmax-xmin
		self.width = width
		self.y_range = width
	
	def make_bars(self, x):
		y = ((x-self.xmin)/self.x_range)*self.y_range
		middle = self.width/2
		y = min(y,self.width)
		y = max(y,0)
		if y<middle:
			left_space = [' ']*int(y)
			bar = ['-']*int(middle-y)
			right_space = [' ']*int(middle)
		elif y>middle:
			left_space = [' ']*int(middle)
			bar = ['-']*int(y-middle)
			right_space = [' ']*int(self.width-y)
		else:
			left_space = [' ']*int(middle)
			bar = ['|']
			right_space = [' ']*int(middle)
		return ''.join(left_space+bar+right_space)

	def run(self, inp):
		x = np.fromstring(inp['data'], dtype=np.float32)
		print self.make_bars(x)

class RoiBars(Stimulus):
	def __init__(self):
		super(RoiBars, self).__init__(**kwargs)
		self.fig = plt.figure();
		self.ax = self.fig.add_subplot(111);
		self.rects = None
		plt.show()
		plt.draw()
	def run(self, data):
		logger.info('running RoiBars with data %s'%data)
		data = json.loads(data)
		if self.rects is None:
			logger.info('self.rects is None')

			self.rects = self.ax.bar(range(len(data)), data.values())
			plt.show()
			plt.draw()
		else:
			logger.info('self.rects is not None')
			for r, v in zip(self.rects, data.values()):
				logger.info('setting %s %f' % (r.__repr__(), v))
				r.set_height(v)
			plt.show()
			plt.draw() # should update

class WeirdSound(Stimulus):
	def __init__(self, **kwargs):
		super(WeirdSound, self).__init__(**kwargs)
		self.server = pyo.Server(audio='jack').boot()
		self.server.start()

		self.lfo_freq0 = 0.4
		self.lfo_freq = pyo.SigTo(value=self.lfo_freq0, time=0.5)
		self.lfo = pyo.LFO(freq=self.lfo_freq, mul=0.005)
		self.lfo.play()
		
		self.f0 = 180
		self.freq = pyo.SigTo(value=[self.f0, self.f0+(0.01*self.f0),
			self.f0*2, self.f0*2+(0.01*self.f0*2)],
			time=1.)

		self.synth = pyo.Sine(freq=self.freq, mul=self.lfo)
		self.pan = pyo.SigTo(value=0.5, time=1.75)
		self.panner = pyo.Pan(self.synth, outs=2, pan=self.pan)
		self.panner.out()

		if self.record:
			rec_path = os.path.join(rec_dir, self.recording_id, 'weirdsound.wav')
			try:
				os.makedirs(os.path.dirname(rec_path))
			except OSError:
				pass
			self.server.recstart(rec_path)

	def run(self, inp):
		if 'pan' in inp:
			pan = np.fromstring(inp['pan'], dtype=np.float32)
			self.pan.value = float(pan)
		# cv1 = controls['M1H']
		# if not np.isnan(cv1):
		# 	f = self.f0*(1.+cv1*2.)
		# 	self.freq.value = [f, f+(0.01*f), f*2, f*2+(0.01*f*2)]

		# cv2 = controls['M1F']
		# if not np.isnan(cv2):
		# 	f = self.lfo_freq0*(1.+cv2*5.)
		# 	self.lfo_freq.value = [f, f+(0.01*f)]
	def stop(self):
		logger.info('stopping weird sound')
		if self.record:
			self.server.recstop()

class Debug(Stimulus):
	def run(self, data):
		data = np.fromstring(data, dtype=np.float32)
		logger.info(data)