import requests
import argparse
from bs4 import BeautifulSoup
from typing import List
import json

class BlobData(object):
    def __init__(self, name, url, lastmodified, etag, size, contenttype, contentencoding, contentlanguage):
        self.name = name
        self.url = url
        self.lastmodified = lastmodified
        self.etag = etag
        self.size = size
        self.contenttype = contenttype
        self.contentencoding = contentencoding
        self.contentlanguage = contentlanguage

class Container(object):
    def __init__(self, container_url):
        self.container_url = container_url

    def get_blobs(self) -> List[BlobData]:
        blobs = []
        resp = requests.get(self.container_url + '?restype=conainer&comp=list')
        if (resp.status_code != 200):
            raise Exception("Failed to list blobs. Status code: " + str(resp.status_code))
        if (not resp.text.startswith('<?xml version="1.0" encoding="utf-8"?><EnumerationResults')):
            raise Exception('Failed to list blobs. Unexpected response content')
        doc = BeautifulSoup(resp.text, 'lxml')
        blobnodes = doc.find_all('blob')
        for n in blobnodes:
            blob = BlobData(
                n.findChild('name').text,
                n.findChild('url').text,
                n.findChild('lastmodified').text,
                n.findChild('etag').text,
                n.findChild('size').text,
                n.findChild('contenttype').text,
                n.findChild('contentencoding').text,
                n.findChild('contentlanguage').text)
            blobs.append(blob)
        return blobs

def ls(blobs: List[BlobData]):
    for b in blobs:
        line = '{0}\t{1:15}\t{2}'.format(b.lastmodified, b.size, b.name)
        print(line)

def print_urls(blobs: List[BlobData]):
    for b in blobs:
        print(b.url)

def print_json(blobs: List[BlobData]):
    print(json.dumps([blob.__dict__ for blob in blobs], indent=2))

output_options = {'ls': ls, 'json': print_json, 'url': print_urls}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help='Storage container url', required=True)
    parser.add_argument('-f', '--format', help='Output format', choices=output_options.keys(), default='ls')
    args = parser.parse_args()

    container = Container(args.url)
    try:
        blobs = container.get_blobs()
    except Exception as e:
        print(e)
        exit(1)
    output_options[args.format](blobs)