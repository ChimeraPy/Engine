from typing import List, Dict, Sequence
import multiprocessing as mp
import collections

# Third-party imports
import tqdm
import pandas as pd

# PyMMDT Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class DataPipelineProcess(mp.Process):

    def __init__(self, logdir, queue, windows, time_window, unique_users, users_meta, verbose=False):
        super().__init__()
        self.logdir = logdir
        self.queue = queue
        self.windows = windows
        self.time_window = time_window
        self.unique_users = unique_users
        self.users_meta = users_meta
        self.verbose = verbose
    
    def load_data_streams(self, unique_users:List, users_meta:Dict) -> Dict[str, Sequence[mm.DataStream]]:

        # Loading data streams # TODO!
        users_data_streams = collections.defaultdict(list)
        for user_name, user_meta in tqdm.tqdm(zip(unique_users, users_meta), disable=not self.verbose):
            for index, row in user_meta.iterrows():
                # Extract useful meta
                entry_name = row['entry_name']
                dtype = row['dtype']

                # Construct the directory the file/directory is found
                if row['is_subsession']:
                    file_dir = self.logdir / row['user']
                else:
                    file_dir = self.logdir
                
                # Load the data
                if dtype == 'video':
                    ds = mmv.VideoDataStream(
                        name=entry_name,
                        start_time=row['start_time'],
                        video_path=file_dir/f"{entry_name}.avi"
                    )
                elif dtype == 'image':

                    # Load the meta CSV
                    df = pd.read_csv(file_dir/entry_name/'timestamps.csv')

                    # Load all the images into a data frame
                    img_filepaths = []
                    for index, row in df.iterrows():
                        img_fp = file_dir / entry_name / f"{row['idx']}.jpg"
                        img_filepaths.append(img_fp)
                        # imgs.append(mm.tools.to_numpy(Image.open(img_fp)))
                    df['img_filepaths'] = img_filepaths
                    
                    # Create ds
                    ds = mmt.TabularDataStream(
                        name=entry_name,
                        data=df,
                        time_column='_time_'
                    )
                elif dtype == 'tabular':
                    raise NotImplementedError("Tabular visualization is still not implemented.")
                else:
                    raise RuntimeError(f"{dtype} is not a valid option.")

                # Store the data in Dict
                users_data_streams[user_name].append(ds)

        # Return the Dict[str, List[DataStream]]
        return users_data_streams

    def run(self):

        print("STARTED")
        users_data_streams = self.load_data_streams(self.unique_users, self.users_meta)
        print("FINISHED LOADING DSS")
        collector = mm.Collector.empty()
        print("FINISHED CREATING COLLECTOR")
        collector.set_data_streams(users_data_streams, self.time_window)
        print("FINISHED SETTING DSS")

        window = self.windows[0]
        start, end = window.start, window.end
       
        print("GETTING")
        data  = collector.get(start, end)
        timetrack = collector.get_timetrack(start, end)
        print("GOT")

        data_chunk = {
            # 'start': start,
            # 'end': end,
            'data': data,
            'timetrack': timetrack
        }

        print("PUTTING")
        self.queue.put(data_chunk)

        print("DONE")
