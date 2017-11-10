import csv
from collections import deque
import elasticsearch
from elasticsearch import helpers


def readEntry():
    try:
        reader = csv.reader(open("logstash-ex.log"), delimiter=" ")
        for line in reader:
            print(line)
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


es = elasticsearch.Elasticsearch()

es.indices.delete(index="grids", ignore=404)
deque(helpers.parallel_bulk(es, readEntry(), index="grids", doc_type="entry"), maxlen=0)
es.indices.refresh()
