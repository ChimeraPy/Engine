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

import analysis
import utils
import debugger

debugger.initialize_flask_server_debugger_if_needed()

app=Flask(__name__)

def step():

    # First, load the data for all the participants
    ps = analysis.data_prep()

    # Get the first participant
    p = ps['P01']
    collector = p['collector']
    analyzer = p['analyzer']

    for i, sample in tqdm.tqdm(enumerate(collector), total=len(collector)):

        # Take a step
        analyzer.step(sample)

        # Extract what we want visualize
        if sample.dtype == 'video':

            # Obtain the drawn_video sample
            drawn_video_sample = analyzer.session.records['drawn_video']

            # For now, let's slow down the process 
            time.sleep(0.033)

            # Extract the frame from the data sample
            frame = drawn_video_sample.data

            # Return the frame in the form of bytes
            yield utils.convert_frame_to_bytes(frame)

    return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/video')
def video():
    return Response(step(), mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__=="__main__":

    # Current debugging form
    # generator = step()
    # while True:
    #     out = next(generator)
    #     if type(out) == type(None):
    #         break

    app.run(
        debug=True,
        host="0.0.0.0",
        port=5000
    )
