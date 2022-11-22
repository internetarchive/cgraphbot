from wikibaseintegrator import WikibaseIntegrator, wbi_login, wbi_helpers
from wikibaseintegrator.datatypes import ExternalID, Item, String, Time
from wikibaseintegrator.wbi_enums import WikibaseDatePrecision
from wikibaseintegrator.wbi_config import config as wbi_config
import credentials
import json
import psycopg2
import requests
import sys
import urllib.parse

dbhost = credentials.postgres_host
dbport = credentials.postgres_port
dbname = credentials.postgres_db
dbuser = credentials.postgres_user
dbpass = credentials.postgres_password

wbi_config['USER_AGENT'] = 'Citationgraph bot/2.0 (https://www.wikidata.org/wiki/User:Harej)'

wikidata_endpoint = 'https://query.wikidata.org/sparql?format=json&query='
wd_entity_prefix = 'http://www.wikidata.org/entity/'
openalex_prefix = 'https://openalex.org/'
fatcat_refurl = 'https://api.fatcat.wiki/v0/release/{0}'

def mediawiki_api(data, login):
    return wbi_helpers.mediawiki_api_call_helper(
        data=data, login=login, allow_anonymous=False)

def connect_to_db():
    conn = psycopg2.connect(\
        'host={0} port={1} dbname={2} user={3} password={4}'\
        .format(dbhost, dbport, dbname, dbuser, dbpass))

    cur = conn.cursor()

    return conn, cur

def get_openalex_cites(work_id):
    conn, cur = connect_to_db()
    q = 'select referenced_work_id from openalex.works_referenced_works ' +\
        'where work_id = \'' + openalex_prefix + work_id + '\';'
    #print(q)
    cur.execute(q)
    manifest = cur.fetchall()

    return [x[0] for x in manifest]

def sparql_query(endpoint, query):
    url = endpoint + urllib.parse.quote(query)
    r = requests.get(url)
    try:
        r = r.json()
    except:
        print(r)
    return r

def identifier_to_wikibase(endpoint, entity_prefix, prop_nr):
    query = 'select ?i ?ident where { ?i wdt:' + prop_nr + ' ?ident . }'
    results = sparql_query(endpoint, query)['results']['bindings']
    return {x['ident']['value']: x['i']['value'].replace(entity_prefix, '')\
               for x in results}

def login_as(username, password):
    return wbi_login.Clientlogin(user=username, password=password)

def build_item_statement(prop, val, reflist):
    return Item(prop_nr=prop, value=val, references=reflist)

def build_extid_statement(prop, val, reflist):
    return ExternalID(prop_nr=prop, value=val, references=reflist)

def build_reflist(stated_in, reference_url, retrieval_date, heuristic):
    return [
        [
            Item(prop_nr='P248', value=stated_in),
            Item(prop_nr='P854', value=reference_url),
            Time(prop_nr='P813', time=retrieval_date,
                     precision=WikibaseDatePrecision.DAY),
            Item(prop_nr='P887', value=heuristic)
        ]
    ]

def update_cites_work():
    # Incomplete
    login = login_as('Citationgraph bot', credentials.citationgraph_bot)
    openalex_to_wikidata = identifier_to_wikibase(
                               wikidata_endpoint, wd_entity_prefix, 'P10283')

    for openalex_id, wikidata_id in openalex_to_wikidata.items():
        # Filter out non-works
        if openalex_id[0] != 'W':
            continue
        works_cited = get_openalex_cites(openalex_id)
        works_cited = [x.replace(openalex_prefix, '') for x in works_cited]
        works_cited = [openalex_to_wikidata[x] for x in works_cited\
                       if x in openalex_to_wikidata]
        if len(works_cited) > 0:
            # Filter against statements already in item (that have strong refs)
            # Then create a function to filter out weak refs where a strong ref
            # exists.
            print(wikidata_id + ': ' + ', '.join(works_cited))

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

def clean_ext_id(prop_nr, ext_id_value):
    if prop_nr == 'P356':
        return ext_id_value.upper()
    elif prop_nr in ['P932', 'P1184']:
        return ext_id_value.replace('PMC', '')
    else:
        return ext_id_value

def sync_fatcat_premapped(filename):
    login = login_as('Identifier sync bot', credentials.identifier_sync_bot)
    wbi = WikibaseIntegrator(login=login)

    with open(filename) as f:
        for line in f:
            record = json.loads(line)
            ref = fatcat_refurl.format(record['P8608'])
            reflist = build_reflist(
                'Q59296908', ref, '+2022-07-30T00:00:00Z', 'Q115331822')
            if 'wikidata' in record:
                wikidata_id = record['wikidata']
                del record['wikidata']
                item = wbi.item.get(wikidata_id)
                pre_edit = item.get_json()
                for prop_nr, ext_id_value in record.items():
                    ext_id_value = clean_ext_id(prop_nr, ext_id_value)
                    statement = build_extid_statement(
                                    prop_nr, ext_id_val, reflist)
                    item.add_claims(statement)
                if pre_edit != item.getjson():
                    item.write(summary="Adding identifiers from fatcat")
                    print(wikidata_id)

def clean_up_cites_works():
    login = login_as('Citationgraph bot', credentials.citationgraph_bot)
    wbi = WikibaseIntegrator(login=login)

    blcontinue = None
    while True:
        apirequest = {
            'action': 'query',
            'list': 'backlinks',
            'bltitle': 'Property:P2860',
            'blnamespace': '0',
            'bllimit': '5000',
        }

        if blcontinue is not None:
            apirequest['blcontinue'] = blcontinue

        api_results = mediawiki_api(apirequest, login)

        items = [x['title'] for x in api_results['query']['backlinks']]

        for wikidata_id in items:
            change_made = False
            item = wbi.item.get(wikidata_id)
            try:
                claims = item.claims.get('P2860')
            except:
                continue
            claims_with_qualifiers = {}
            # First, we note the P2860 claims with qualifiers. These
            # are unusual so we are noting them.
            #print(wikidata_id)
            for claim in claims:
                if 'value' in claim.mainsnak.datavalue:
                    cited_work = claim.mainsnak.datavalue['value']['id']
                    if len(claim.qualifiers) > 0:
                        claims_with_qualifiers[cited_work] = claim.id
            # Now we are checking claims with references, which would be
            # claims this bot would have added. If the same cited work
            # appears in the above dict and it's not because they have
            # the same claim ID, that signals a duplicate entry.
            for claim in claims:
                if len(claim.references) > 0 and len(claim.qualifiers) == 0:
                    if 'value' in claim.mainsnak.datavalue:
                        cited_work = claim.mainsnak.datavalue['value']['id']
                        if cited_work in claims_with_qualifiers:
                            if claims_with_qualifiers[cited_work] != claim.id:
                                # Identify the reference from the redundant
                                # claim, add it to the qualified claim, and
                                # delete the redundant claim.
                                good_reference = claim.references.references[0]
                                other_claim_id = claims_with_qualifiers[cited_work]
                                # I don't like this nested loop, but we are
                                # dealing with a flat list here.
                                for other_claim in claims:
                                    if other_claim.id == other_claim_id:
                                        other_claim.references.add(good_reference)
                                        break
                                claim.remove()
                                change_made = True
            if change_made is True:
                item.write(summary='Combining redundant P2860 statements')
                print(wikidata_id)

        if 'continue' not in api_results:
            print('Done')
            break
        else:
            blcontinue = api_results['continue']['blcontinue']

if __name__ == '__main__':
    command = sys.argv[1]
    if command == 'clean_up_cites_works':
        clean_up_cites_works()
    elif command == 'sync_fatcat_premapped':
        sync_fatcat_premapped('fatcat-identifiers-2022-07-30.jsonl')