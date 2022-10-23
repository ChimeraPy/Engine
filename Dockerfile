# syntax=docker/dockerfile:1
#FROM ubuntu:22.04
FROM python:3.8-slim-buster

# Run dependency installation
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6 -y

# Copy the Local repo
RUN ls
COPY . /ChimeraPy
RUN ls

# Install ChimeraPy
RUN python3 -m pip install --upgrade pip
RUN cd ChimeraPy && python3 -m pip install '.[test]'
