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
            raise Exception("Failed to get list blobs")
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

def print_urls(blobs):
    for b in blobs:
        print(b.url)

def print_json(blobs):
    print(json.dumps([blob.__dict__ for blob in blobs], indent=2))

output_options = {'ls': ls, 'json': print_json, 'url': print_urls}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help='Storage container url', required=True)
    parser.add_argument('-f', '--format', help='Output format', choices=output_options.keys(), default='ls')
    args = parser.parse_args()

    container = Container(args.url)
    blobs = container.get_blobs()
    output_options[args.format](blobs)