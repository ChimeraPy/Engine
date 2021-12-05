"""
Other resources: 
https://github.com/krishnaik06/Flask-Web-Framework/blob/main/Tutorial%207/app.py
https://stackoverflow.com/a/67856716/13231446
https://community.plotly.com/t/does-dash-support-opencv-video-from-webcam/11012

app.py expects to be ran from the root GitHub directory.

"""
import os
import time

from flask import Flask, render_template, Response
import tqdm
import cv2

import pymmdt.webapp.debugger as db

db.initialize_flask_server_debugger_if_needed()

app=Flask(__name__)

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/video')
# def video():
#     return Response(step(), mimetype='multipart/x-mixed-replace; boundary=frame')

def main():
    print("Running App")
    # app.run(
    #     debug=True,
    #     host="0.0.0.0",
    #     port=5000
    # )

if __name__=="__main__":
    main()
