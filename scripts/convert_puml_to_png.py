"""
@author: Eduardo Davalos (eduardo.davalos.anaya@vanderbilt.edu)
@date: 11/08/2021

Purpose:
This script converts all the .puml files (which contain PlantUML) 
diagrams into .png images. The .puml files are located in ``plantuml``
directory in the ``docs``. The goal is to automaticall generate images
and store them in the ``imgs`` subdirectory of the documentation.

"""

import os
import pathlib

import plantuml

# Create a plantuml server object
server = plantuml.PlantUML(
    url='http://www.plantuml.com/plantuml/img/',
    basic_auth={},
    form_auth={},
    http_opts={},
    request_opts={}
)

# Determine the diagram and images directories
script_path = pathlib.Path(os.path.abspath(__file__))
git_repo_dir = script_path.parent.parent
plantuml_dir = git_repo_dir / 'docs' / 'plantuml'
imgs_dir = git_repo_dir / 'docs' / 'imgs'

# Process files in the diagrams director of the documentation
for file in plantuml_dir.iterdir():
    if file.suffix == '.puml':
        output_file = imgs_dir / (file.stem + '.png')
        server.processes_file(str(file), str(output_file))
