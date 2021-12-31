"""Utils package is a collection of common tools to use alongside PyMMDT.

"""

# Package Management
__package__ = 'utils'

# Built-in Imports
from typing import Dict, Sequence
import ast
import pathlib
import gzip
import json

# Third-Party Imports
import pandas as pd
import tqdm

# Built-in Imports
from pymmdt.data_stream import DataStream
from pymmdt.video.data_stream import VideoDataStream
from pymmdt.tabular.data_stream import TabularDataStream

# Loading data from files
def load_g3_file(gz_filepath:pathlib.Path) -> Dict:

    # Load the data from file 
    with open(gz_filepath, mode='rt') as f:
        data = f.read()

    # Convert (false -> False, true -> True, null -> None)
    safe_data = data.replace('false', 'False').replace('true', 'True').replace('null', 'None')

    # Convert the data into dict
    data_dict = ast.literal_eval(safe_data)

    # Return dict
    return data_dict

def load_temporal_gz_file(gz_filepath:pathlib.Path, verbose:bool=False) -> pd.DataFrame:
   
    # Convert the total_data to a dataFrame
    df = pd.DataFrame()

    # Load the data from file 
    with gzip.open(gz_filepath, mode='rt') as f:
        data = f.read()

    data_lines = data.split('\n')

    for line in tqdm.tqdm(data_lines, disable=not verbose):

        # Drop empty lines
        if line == '':
            continue

        data_dict = ast.literal_eval(line)
        data = data_dict.pop('data')

        # Skip if the data is missing
        if data == {}:
            continue

        data_dict.update(data)

        insert_df = pd.DataFrame([data_dict])
        df = df.append(insert_df)

    # Clean the index 
    df.reset_index(inplace=True)
    df = df.drop(columns=['index'])

    # Return the data frame
    return df

# Loading data for participants
def load_participant_data(dir:pathlib.Path, verbose:bool=False) -> Sequence[DataStream]:

    # Before trying to original data format, check if the faster csv 
    # version of the data is available
    gaze_df_path = dir / 'gazedata.csv'
    imu_df_path = dir / 'imudata.csv'

    # Loading data, first if csv form, latter with original
    if gaze_df_path.exists():
        gaze_data_df = pd.read_csv(gaze_df_path)
    else:
        gaze_data_df = load_temporal_gz_file(dir / 'gazedata.gz', verbose=verbose)
        gaze_data_df.to_csv(gaze_df_path, index=False)

    if imu_df_path.exists():
        imu_data_df = pd.read_csv(imu_df_path)
    else:
        imu_data_df = load_temporal_gz_file(dir / 'imudata.gz', verbose=verbose)
        imu_data_df.to_csv(imu_df_path, index=False)

    recording_specs_df = load_g3_file(dir / 'recording.g3')

    # Convert the time column to pd.TimedeltaIndex
    gaze_data_df['_time_'] = pd.to_timedelta(gaze_data_df['timestamp'], unit='s')
    imu_data_df['_time_'] = pd.to_timedelta(imu_data_df['timestamp'], unit='s')

    # Convert the data to DataStream
    gaze_ds = TabularDataStream(
        name='gaze',
        data=gaze_data_df, 
        time_column='_time_',
    )
    video_ds = VideoDataStream(
        name='video',
        video_path=dir/'scenevideo.mp4',
        start_time=pd.Timedelta(0),
    )

    # Storing recording ID
    participant_data = [gaze_ds, video_ds]

    return participant_data


def load_session_data(data_dir:pathlib.Path, verbose:bool=False) -> Dict[str, Dict]:

    # Load the participant IDs
    with open(data_dir / 'meta.json') as f:
        meta_json = json.load(f)

    # Loading all participant datas
    p_meta = meta_json['PARTICIPANT_ID']
    ps = {}
    for p_id, p_dir in p_meta.items():

        # Create complete path
        complete_p_dir = data_dir / p_dir

        # Loading each participant data
        p_data = load_participant_data(complete_p_dir, verbose=verbose)

        # Store the participant data
        ps[p_id] = {'data': p_data}

    # Return all the participants data
    return ps
