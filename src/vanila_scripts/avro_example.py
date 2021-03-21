import gzip
import json
import logging
import base6
from io import BytesIO

import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader

def read_avro_file(file_path=None) -> pd.DataFrame{
    try:
        bytes_holder = BytesIO()
        with open(file_path, "rb") as avro_fh:
            bytes_holder.write(avro_fh.read())
        avro_reader = DataFileReader(bytes_holder, DatumReader())
        data = [gzip.decompress(element['Body']) for element in avro_reader]

        for element in data:
            current_list = element.decode('utf-8').split("\n")
            new_list = []
            [new_list.append(item) if item.strip() else False for item in current_list]
        return pd.DataFrame(new_list)
    except Exception as err:
        return None
}