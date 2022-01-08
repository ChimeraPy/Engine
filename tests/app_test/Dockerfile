from continuumio/miniconda3

# Setting up the environment
ENV QT_DEBUG_PLUGINS=1
ENV APP_HOME /app_test
WORKDIR $APP_HOME
COPY . $APP_HOME

# Creating all the necessary packages and dependencies
RUN apt-get update
# RUN apt-get install qt5-default
# RUN apt-get install qtbase5-dev qtchooser qt5-qmake qtbase5-dev-tools
RUN apt-get install --reinstall libxcb-xinerama0
RUN apt-get install ffmpeg libsm6 libxext6 -y
RUN apt-get install libxcb-icccm4 -y

COPY environment.yml .
RUN conda env create -f environment.yml

SHELL ["conda", "run", "--name", "app", "/bin/bash", "-c"]
ENTRYPOINT ["conda", "run", "--name", "app", "python", "app.py"]
