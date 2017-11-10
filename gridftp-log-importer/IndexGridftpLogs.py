import csv
from collections import deque
import elasticsearch
import os
from elasticsearch import helpers

# Set the directory in which the log files are stored
dir_list = os.fsencode("/home/dna/gftplogs")


def readEntry():
    for filename in os.listdir(dir_list):
        # Create the complete file path.
        file = open(os.path.join(dir_list, filename))
        try:
            reader = csv.reader(file, delimiter=" ")
            for line in reader:
                # Create a Single Entry.
                entry = {}
                entry['timestamp'] = line[0]
                entry['source'] = line[1]
                entry['event'] = line[2]
                entry['src_ip'] = line[3]
                entry['src_port'] = int(line[4])
                entry['username'] = line[5]
                entry['filename'] = line[6]
                entry['direction'] = line[7]
                yield entry
        except:
            print("ERROR: File Not Found!")
        finally:
            file.close()


# Use the ES APIs to delete existing index and create a new index.
es = elasticsearch.Elasticsearch()

es.indices.delete(index="grids", ignore=404)

# Use the bulk input API.
deque(helpers.parallel_bulk(es, readEntry(), index="grids", doc_type="entry"), maxlen=0)
es.indices.refresh()
