# syntax=docker/dockerfile:1
FROM python:3.9

# Run dependency installation
RUN apt-get update

# For OpenCV
RUN apt-get install ffmpeg libsm6 libxext6 -y
# For PyAudio
RUN apt-get install libportaudio2 libportaudiocpp0 portaudio19-dev libasound-dev libsndfile1-dev -y
RUN apt-get install portaudio19-dev python3-pyaudio -y

# Copy the Local repo
RUN ls
COPY . /ChimeraPy
RUN ls

# Install ChimeraPy
RUN python3 -m pip install --upgrade pip
RUN cd ChimeraPy && python3 -m pip install '.[test]' && cd ..

# For a certain test, remove the mock
RUN rm -r ChimeraPy/test/mock/test_package
