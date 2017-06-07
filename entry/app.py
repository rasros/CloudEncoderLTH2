#!flask/bin/python
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify 
import uuid


app = Flask(__name__)

@app.route('/',methods=['POST'])
def index():
    videoFile = request.files['file'];
    theID = uuid.uuid4()
    videoFile.save(str(theID) + ".mp4")
    resp = Response()
    resp.headers['Location'] = '/' + str(theID)
    return resp,201

index = 0

@app.route('/<uuid>/status')
def status(uuid):
    global index
    if(index < 1):
        res = { 'status' : 'QUEUED' , 'progress' : 0 }
    elif(index < 5):
        res = { 'status' : 'PROCESSING' , 'progress' : 50 }
    else:
        res = { 'status' : 'DONE' , 'progress' : 100 }

    index = index + 1

    return jsonify(res)


@app.route('/<uuid>/download')
def download(uuid):
    resp = Response()
    resp.headers['Content-disposition'] = 'attachment; filename=' + uuid + '.mp4'
    theFile = open(uuid + '.mp4','rb')
    resp.data = theFile.read()
    index = 0
    return resp

if __name__ == '__main__':
    app.run(debug=True)
