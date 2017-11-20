import csv
from collections import deque
import elasticsearch
import os
from elasticsearch import helpers
from dateutil import parser

# Set the directory in which the log files are stored
dir_list = os.fsencode("/home/dna/gftplogs")


def readEntry():
    for filename in os.listdir(dir_list):
        # Create the complete file path.
        file = open(os.path.join(dir_list, filename))
        # print("Indexing File: " + str(filename, 'utf-8'))
        try:
            reader = csv.reader(file, delimiter=" ")
            for line in reader:
                # Create a Single Entry.
                timestamp = parser.parse(line[0])
                entry = {'@timestamp': timestamp, 'source': line[1], 'event': line[2], 'src_ip': line[3],
                         'src_port': int(line[4]), 'username': line[5], 'filename': line[6], 'direction': line[7]}
                yield entry
                # print(entry)
        except:
            print("ERROR: File Not Found!")
        finally:
            file.close()


# Use the ES APIs to delete existing index and create a new index.
es = elasticsearch.Elasticsearch()

print("Deleting previously created index...")
es.indices.delete(index="grids", ignore=404)
print("Deleted.")

# Use the bulk input API.
print("Creating new Index...")
deque(helpers.parallel_bulk(es, readEntry(), index="grids", doc_type="entry"), maxlen=0)

print("Refreshing Indices...")
es.indices.refresh()
print("Done.")
