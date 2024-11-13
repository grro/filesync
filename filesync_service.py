import sys
import logging
import traceback
import os
import yaml
import pycron
from datetime import datetime
from time import sleep
from typing import Dict, Any, List
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from filesync import sync_folder, Progress
from display import Display, RemoteDisplay



class FileHandler(FileSystemEventHandler):

    def __init__(self, change_listener):
        self.__change_listener = change_listener

    def on_modified(self, event) -> None:
        self.__change_listener()





class Task:

    def __init__(self, conf: Dict[str, Any]):
        self.__conf = conf

    @property
    def source(self) -> str:
        return self.__conf['source']

    @property
    def target(self) -> str:
        return self.__conf['target']

    @property
    def ignore_patterns(self) -> List[str]:
        return self.__conf.get('ignore_patterns', ['*/~*'])

    @property
    def ignore_lastmodified(self) -> bool:
        return self.__conf.get('ignore_lastmodified', False)

    @property
    def ignore_filesize(self) -> bool:
        return self.__conf.get('ignore_filesize', False)

    @property
    def ignore_subdirs(self) -> bool:
        return self.__conf.get('ignore_subdirs', False)

    @property
    def ignore_hash(self) -> bool:
        return self.__conf.get('ignore_hash', False)

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return self.source + "->" + self.target




class Config:

    def __init__(self, file: str, conf: Dict[str, Any]):
        self.file = file
        self.cron = conf['cron']
        self.display = conf.get('display', "")
        self.simulate = conf.get('simulate', False)
        self.tasks = [Task(task) for task in conf['tasks']]

    def __hash__(self):
        return hash(self.cron + ",".join([str(task) for task in self.tasks]))



class Sync(Progress):

    def __init__(self, config: Config, workdir: str):
        self.num_up = 0
        self.num_down = 0
        self.config = config
        self.workdir = workdir
        self.display = Display() if len(config.display) == 0 else RemoteDisplay(config.display)

    def execute(self):
        for task in self.config.tasks:
            self.display.show("sync\n\r" + task.target.split("/")[-1] + "...")
            sync_folder(source_address=task.source,
                        target_address=task.target,
                        ignore_lastmodified=task.ignore_lastmodified,
                        ignore_filesize=task.ignore_filesize,
                        ignore_patterns=task.ignore_patterns,
                        ignore_hash=task.ignore_hash,
                        ignore_subdirs=task.ignore_subdirs,
                        progress=self,
                        workdir=self.workdir,
                        simulate=self.config.simulate)
        self.display.show(datetime.now().strftime("%d %b, %H:%M") + "\n\r" + str(self.num_down) + " down; " +  str(self.num_up) + " up")

    def on_uploaded(self, filename: str):
        self.num_up += 1
        self.display.show("sync...\n\r" + filename)

    def on_downloaded(self, filename: str):
        self.num_down += 1
        self.display.show("sync...\n\r" + filename)



class FilesyncService:

    def __init__(self, dir: str):
        self.__is_running = True
        self.dir = dir
        self.observer = Observer()
        self.configs = list()

    def start(self):
        self.__is_running = True
        self.observer.schedule(FileHandler(self.__reload), self.dir, recursive=False)
        self.observer.start()
        self.__reload()
        self.__cron_loop()


    def close(self):
        self.__is_running = False
        self.observer.stop()

    def __reload(self):
        new_configs = set()
        for f in os.scandir(self.dir):
            fn = os.path.join(self.dir, f.name)
            try:
                if f.is_file() and f.name.endswith(".yml"):
                    with open(fn, 'r') as file:
                        yml = yaml.safe_load(file)
                        config = Config(file.name, yml)
                        new_configs.add(config)
                        logging.info(f.name + " reloaded")
            except Exception as e:
                logging.warning("error occurred by loading " + str(fn) + " "  + str(e))
        self.configs = new_configs

    def __cron_loop(self):
        while self.__is_running:
            for config in self.configs:
                try:
                    if pycron.is_now(config.cron):
                        Sync(config, self.dir).execute()
                except Exception as e:
                    logging.warning("Error occurred processing snyc for " + config.file + "  " + str(e))
                    print(traceback.format_exc())
            sleep(40)  # <60 and >30


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    srv = FilesyncService(sys.argv[1])
    srv.start()
