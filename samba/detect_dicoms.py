#!/usr/bin/python
import logging
import os
import os.path as op
import glob
from pathlib import Path
import pwd
import re
import subprocess
import time
from typing import Dict
from collections import defaultdict

import redis
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

# SETUP LOGGING
logger = logging.getLogger('samba.detect_dicoms')
logger.setLevel(logging.DEBUG)
LOG_FORMAT = '%(asctime)-12s %(name)-20s %(levelname)-8s %(message)s'
formatter = logging.Formatter(LOG_FORMAT)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
fh = logging.FileHandler('/logs/samba.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

user = pwd.getpwuid(os.getuid()).pw_name
logger.info("Running as user %s", user)


def detect_dicoms(root_directory=None, extension='*'):
    """Continuously monitor a samba mounted directory for new files and publish new paths.

    File creation on samba network shares do not trigger the same inotify events as regular files.
    This function monitors a samba shared directory for new files. When a new file is detected,
    ensure it is closed, then publish the name over redis.

    Parameters
    ----------
    directory : str
        Directory to monitor for new files
    extension : str
        Only detect new files with this extension
    """
    logger.info('Monitoring %s', root_directory)

    monitor = MonitorSambaDirectory(root_directory, file_glob="*/*" + extension)
    monitor = MonitorDirectory(root_directory, file_glob="*/*" + extension)

    r = redis.StrictRedis('redis')

    for new_path in monitor.yield_new_paths():
        new_path = new_path.replace(root_directory, '', 1).lstrip('/')
        file_size_kib = os.path.getsize(root_directory + '/' + new_path) / (1<<10)
        logger.info(f'SAMBA got a new volume {new_path} at time {time.time()} with size {file_size_kib:.3f} MB')
        r.publish('volume', new_path)

class MonitorDirectory(object):
    """
    Monitor the file contents of a directory via continuous polling

    Parameters
    ----------
    directory : str
        The directory to monitor
    file_glob : str

    Examples
    --------
    Loop that iterates each time a file is detected.

    >>> m = MonitorDirectory('/tmp/test', file_glob='*/*.dcm')
    >>> for path in m.yield_new_paths():
    >>>     print(path)
    /tmp/test/1.dcm
    /tmp/test/2.dcm
    ...
    """

    def __init__(self, root_directory, file_glob="*/*.dcm"):
        self.root_directory = root_directory
        self.file_glob = file_glob

        self.build()

    def build(self):
        # was this file recently modified? if so, do NOT say this file is
        # ready!
        self.files_recently_modified = {}

        # TODO: rename event handler class
        class MyEventHandler(FileSystemEventHandler):
            def __init__(self, root_directory, file_glob, files_recently_modified):
                self.root_directory = root_directory
                self.file_glob = file_glob
                #self.files_recently_modified = files_recently_modified
                self.files_recently_modified = {}

            def on_any_event(self, event: FileSystemEvent) -> None:
                # TODO: FileModifiedEvent, FileCreatedEvent. may be in any
                # order
                if event.is_directory: return

                if event.event_type in ['modified', 'created']:
                    self.files_recently_modified[event.src_path] = True
                    logger.info('file recently modified: ' + event.src_path)


                # TODO: filter for glob
                logger.info(event)

        self.observer = Observer(timeout=0.1)
        #self.samba_status = SambaStatus(self.root_directory)

        self.event_handler = MyEventHandler(root_directory=self.root_directory,
                                            file_glob=self.file_glob,
                                            files_recently_modified = self.files_recently_modified)
        self.observer.schedule(self.event_handler, self.root_directory, recursive=True)
        self.observer.start()

        self.contents = set()
        self.update_contents()

    def update_contents(self):
        current_files = set(glob.glob(op.join(self.root_directory, self.file_glob)))
        new_files = current_files - self.contents
        deleted_files = self.contents - current_files

        #smb_open_files = self.samba_status.get_open_files()

        resolved_modified_paths = {Path(path).resolve() for path, modified in self.event_handler.files_recently_modified.items() if modified}
        #logger.info('tick')
        if len(self.files_recently_modified) > 0:
            logger.info('recently modified files: ' + str(resolved_modified_paths))

        eligible_new_files = {filename for filename in new_files if \
                              not (Path(filename).resolve() not in resolved_modified_paths)}

        #eligible_new_files = {filename for filename in new_files if filename
        #                      not in smb_open_files}
        self.contents.difference_update(deleted_files)
        self.contents.update(eligible_new_files)

        #self.files_recently_modified.clear() # reset files modified since last tick
        self.event_handler.files_recently_modified.clear() # reset list of files modified since last tick

        return eligible_new_files

    def yield_new_paths(self):
        while True:
            eligible_new_files = self.update_contents()
            for filename in sorted(eligible_new_files, key=op.getmtime):
                logger.info(f"Adding {filename} at {time.time()}")
                yield filename

            time.sleep(.1) # this must be bigger than the PollingObserver's tick


class MonitorSambaDirectory(object):
    """
    Monitor the file contents of a directory mounted with samba share

    Parameters
    ----------
    directory : str
        The directory to monitor
    file_glob : str

    Examples
    --------
    Loop that iterates each time a file is detected.

    >>> m = MonitorSambaDirectory('/tmp/test', file_glob='*/*.dcm')
    >>> for path in m.yield_new_paths():
    >>>     print(path)
    /tmp/test/1.dcm
    /tmp/test/2.dcm
    ...
    """

    def __init__(self, root_directory, file_glob="*/*.dcm"):
        self.root_directory = root_directory
        self.file_glob = file_glob

        self.build()

    def build(self):
        self.samba_status = SambaStatus(self.root_directory)
        self.contents = set()
        self.update_contents()

    def update_contents(self):
        current_files = set(glob.glob(op.join(self.root_directory, self.file_glob)))
        new_files = current_files - self.contents
        deleted_files = self.contents - current_files
        smb_open_files = self.samba_status.get_open_files()
        eligible_new_files = {filename for filename in new_files if filename
                              not in smb_open_files}
        self.contents.difference_update(deleted_files)
        self.contents.update(eligible_new_files)
        return eligible_new_files

    def yield_new_paths(self):
        while True:
            eligible_new_files = self.update_contents()
            for filename in sorted(eligible_new_files, key=op.getmtime):
                logger.info(f"Adding {filename} at {time.time()}")
                yield filename
            time.sleep(.1)


class SambaStatus():
    """Class to access information output by the `smbstatus` command.

    Parameters
    ----------
    directory : str
        Only return information related to this directory
    """
    def __init__(self, directory):
        self.directory = directory
        self.open_file_parser = re.compile("\d*\s*\d*\s*[A-Z_]*\s*0x\d*\s*[A-Z]*\s*[A-Z]*\s*"
                                           "%s\s*(?P<path>.*\.dcm).*" % directory)

    def get_open_files(self):
        """Get a list of files that are currently opened by samba clients
        """
        proc = subprocess.Popen(['smbstatus', '-L'], stdout=subprocess.PIPE)
        proc.stdout.readline()
        proc.stdout.readline()
        proc.stdout.readline()

        paths = []
        for info in proc.stdout.readlines():
            if info != b'\n':
                groups = self.open_file_parser.match(info)
                if groups is not None:
                    path = groups.groupdict()['path']
                    paths.append(op.join(self.directory, path))

        return paths


if __name__ == "__main__":
    detect_dicoms(root_directory='/mnt/scanner', extension='.dcm')
