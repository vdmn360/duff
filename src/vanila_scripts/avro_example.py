import gzip
import json
import logging
import base64
from io import BytesIO

import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader

def read_avro_file(file_path=None):
    try:
        bytes_holder = BytesIO()
        with open(file_path, "rb") as avro_fh:
            bytes_holder.write(avro_fh.read())
        avro_reader = DataFileReader(bytes_holder, DatumReader())
        data = [gzip.decompress(element['Body']) for element in avro_reader]
        df_list = []
        for element in data:
            new_list = []
            current_list = element.decode('utf-8').split("\n")
            new_list = json.loads(current_list[0])
            df_list.append(pd.DataFrame(new_list))

        sensor_df = pd.DataFrame(new_list)
        [sensor_df.drop([column], axis=1, inplace=True) if column[0] == '#' else False for column in sensor_df.columns]
        return pd.concat(df_list)
        #return sensor_df
    except Exception:
        return None

