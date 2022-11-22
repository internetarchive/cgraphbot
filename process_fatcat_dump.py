import gzip
import json

key_to_wdprop = {
    'doi': 'P356',
    'pmid': 'P698',
    'core': 'P6409',
    'wikidata_qid': 'wikidata',
    'doaj': 'P5115',
    'arxiv': 'P818',
    'pmcid': 'P932',
    'dblp': 'P8978',
    'jstor': 'P888',
    'isbn13': 'P212',
    'hdl': 'P1184'
}

def get_id(blob, id_type):
    if id_type in blob:
        return blob[id_type]
    else:
        return None

def process_file(filename):
    ext_id_types = []
    with gzip.open(filename, 'rt') as f:
        for line in f:
            blob = json.loads(line)
            fatcat_id = blob['ident']
            retblob = {'P8608': fatcat_id}
            ext_ids = get_id(blob, 'ext_ids')
            if ext_ids is not None:
                for key, identifier in ext_ids.items():
                    retblob[key_to_wdprop[key]] = identifier
            print(json.dumps(retblob))

if __name__ == '__main__':
    process_file('/home/jh/Downloads/release_export_expanded.json.gz')
