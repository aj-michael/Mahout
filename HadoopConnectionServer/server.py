from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os
from firebase import firebase

PORT_NUMBER = 8002

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        self.wfile.write("Hi there")
        return

    def do_PUT(self):
        length = int(self.headers['Content-Length'])
        content = self.rfile.read(length)
        phrase,alg = map(lambda x:x.split('=')[1],content.split('&'))
        phrase = phrase.replace('+',' ')
        ref.put('','status','starting up')
        print phrase
        print alg
        jobid = 'abdf'
        print jobid
        os.system('hadoop jar /adam/jars/bm25classify.jar ngrams/BM25model "'+phrase'" '+jobid)
         

try:
    ref = firebase.FirebaseApplication('https://mahout.firebaseio.com')
    server = HTTPServer(('',PORT_NUMBER),Handler)
    print 'Started server on port ', PORT_NUMBER
    server.serve_forever()

except KeyboardInterrupt:
    print '\nserver shutting down'
    server.socket.close()
