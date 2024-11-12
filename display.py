import requests


class Display():

    def show(self, msg: str):
        print(msg)


class RemoteDisplay(Display):

    def __init__(self, display_uri: str = None):
        self.display_uri = display_uri.strip("/")

    def show(self, msg: str):
        try:
            if self.display_uri is not None:
                uri = self.display_uri + "/lower_layer_text"
                resp = requests.put(uri, json= {'lower_layer_text': msg })
                #print(resp)
        except Exception:
            print("error updating panel")
