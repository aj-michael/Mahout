import random
import string
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
        print phrase
        phrase = phrase.replace('+',' ')
        #TODO: Uncomment this
        ref.put('','status','starting up')
        print phrase
        print alg
	jobid = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
        print jobid
        #TODO: Uncomment this
	ref.put('','query',phrase)
        if alg == 'NBayes':
            print 'got bayes'
            os.system('hadoop jar /adam/jars/nbclassify.jar "'+phrase+'" websiteoutput/'+jobid)
        elif alg == 'BM25':
            print 'got bm'
            os.system('hadoop jar /adam/jars/bm25classify.jar ngrams/BM25model "'+phrase+'" websiteoutput/'+jobid+' 1.2 0.5')
        elif alg == 'knn':
            print 'got knn'
            os.system('hadoop jar /adam/jars/KNNClassifier.jar "'+phrase+'" /user/root/ngrams/BM25model/filtered /user/root/knnworkspace websiteoutput/'+jobid)
        ref.put('','notthequery',phrase)
        """
        """
         

try:
    ref = firebase.FirebaseApplication('https://mahout.firebaseio.com')
    server = HTTPServer(('',PORT_NUMBER),Handler)
    print 'Started server on port ', PORT_NUMBER
    server.serve_forever()

except KeyboardInterrupt:
    print '\nserver shutting down'
    server.socket.close()
