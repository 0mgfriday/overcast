import requests
import argparse
from bs4 import BeautifulSoup
from typing import List
import json

class BlobData(object):
    def __init__(
        self, 
        name, 
        url,
        snapshot,
        lastmodified, 
        etag, 
        size,
        contenttype,
        contentencoding,
        contentlanguage,
        contentmd5,
        blobtype,
        leasestatus
    ):
        self.name = name
        self.url = url
        self.snapshot = snapshot
        self.lastmodified = lastmodified
        self.etag = etag
        self.size = size
        self.contenttype = contenttype
        self.contentencoding = contentencoding
        self.contentlanguage = contentlanguage
        self.conentmd5 = contentmd5
        self.blobtype = blobtype
        self.leasestatus = leasestatus

class Container(object):
    def __init__(self, container_url):
        self.container_url = container_url

    def get_blobs(self, include_snapshots=False, sas=None, max_results=None) -> List[BlobData]:
        blobs = []
        request_uri = self.container_url + '?restype=container&comp=list'
        if (include_snapshots):
            request_uri += '&include=snapshots'
        if (max_results is not None):
            request_uri += '&maxresults=' + str(max_results)
        if (sas is not None):
            request_uri += '&' + sas
        print(request_uri)
        resp = requests.get(request_uri)
        if (resp.status_code != 200):
            raise Exception("Failed to list blobs. Status code: " + str(resp.status_code))
        if (not resp.text.startswith('<?xml version="1.0" encoding="utf-8"?><EnumerationResults')):
            raise Exception('Failed to list blobs. Unexpected response content')
        doc = BeautifulSoup(resp.text, 'lxml')
        blobnodes = doc.find_all('blob')
        for n in blobnodes:
            properties = n.findChild('properties')
            name = n.findChild('name')
            url = n.findChild('url')
            snapshot = n.findChild('snapshot')
            leasestatus = properties.findChild('leasestatus')
            blob = BlobData(
                name.text,
                url.text if url is not None else self.container_url + '/' + name.text,
                snapshot.text if snapshot is not None else None,
                properties.findChild('last-modified').text,
                properties.findChild('etag').text,
                properties.findChild('content-length').text,
                properties.findChild('content-type').text,
                properties.findChild('content-encoding').text,
                properties.findChild('content-language').text,
                properties.findChild('content-md5').text,
                properties.findChild('blobtype').text,
                leasestatus.text if leasestatus is not None else None)
            blobs.append(blob)
        return blobs

def ls(blobs: List[BlobData], includesnapshots = False):
    for b in blobs:
        if (includesnapshots):
            line = '{0}\t{1:15}\t{2:28}\t{3}'.format(b.lastmodified, b.size, b.snapshot or '', b.name)
        else:
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
    parser.add_argument('-is', '--includesnapshots', help='Include snapshots', action='store_true')
    parser.add_argument('-s', '--sas', help='Shared Access Signatre')
    parser.add_argument('-m', '--max', help='Max results to return', type=int)
    args = parser.parse_args()

    container = Container(args.url)
    try:
        blobs = container.get_blobs(args.includesnapshots, args.sas, args.max)
    except Exception as e:
        print(e)
        exit(1)
    if (args.format == 'ls'):
        ls(blobs, args.includesnapshots)
    else:
        output_options[args.format](blobs)