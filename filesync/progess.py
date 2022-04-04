from datetime import datetime
import requests


class Display():

    def __init__(self, display_uri: str, type: str = " "):
        self.display_uri = display_uri
        self.type = type
        self.num_up = 0
        self.num_down = 0
        self.display('sync ' + self.type + '...')

    def on_uploaded(self):
        self.num_up = self.num_up + 1
        self.display('sync ' + self.type + '...')

    def on_downloaded(self):
        self.num_down = self.num_down + 1
        self.display('sync ' + self.type + '...')

    def close(self):
        self.display(datetime.now().strftime("%d %b, %H:%M"))

    def display(self, msg: str):
        try:
            requests.put(self.display_uri, json= {'lower_layer_text': msg + '\n\rdown: ' + str(self.num_down) + ' up: ' + str(self.num_up) })
        except Exception:
            print("error updating panel")
