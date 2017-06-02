#!/usr/bin/env python

from os import path, environ
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys, threading, codecs

def read_file(f):
    with codecs.open(path.join(path.dirname(__file__), f), "r",
                     encoding = "utf-8") as inp:
        return inp.read()
html = read_file("eula.html").replace("{TEXT}", read_file("EULA.txt"))

class eulaRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/exit-accept":
            httpd.exit_code = 0
            threading.Thread(target = httpd.shutdown, daemon = True).start()
        elif self.path == "/exit-reject":
            httpd.exit_code = 1
            threading.Thread(target = httpd.shutdown, daemon = True).start()
        else:
            self.send_response(200)
            self.send_header("Content-type","text/html")
            self.end_headers()
            message = html
            self.wfile.write(bytes(message, "utf8"))
            return

pvar = "MMLSPARK_JUPYTER_PORT"
port = int(environ[pvar]) if pvar in environ else 8888

print("Running EULA server...")
httpd = HTTPServer(("", port), eulaRequestHandler)
httpd.serve_forever()

print("Done, " + ("accept" if httpd.exit_code == 0 else "reject") + "ing")
sys.exit(httpd.exit_code)
