import os,sys
import collections
import itertools
import datetime
import copy
import json

from aip.libs import merger
from aip.libs import enforce_schema
from aip.db import session_scope
from aip.models import Records, ChangeLog
from aip.libs.utils import get_date

def mergeRecords(records):
    completeRecords = []
    e = enforce_schema.Enforcer() # TODO: no need to create new instances?
    for r in copy.deepcopy(records):
        r['text'] = merger.Merger().mergeText(r['text'])
        blocks = e.ensureList(r['metadata'])
        #Multiply defined blocks need merging.
        metadatablockCounter = collections.Counter([i['tempdata']['type'] for i in blocks])
        needsMerging = dict([(k,[]) for k,v in metadatablockCounter.iteritems() if v>1])
    
        completeMetadata = {}
        #First pass: Add the singly defined blocks to the complete record
        for b in blocks:
            _type = b['tempdata']['type']
            if _type not in needsMerging:
                completeMetadata[_type] = b
            else:
                needsMerging[_type].append(b)
    
    #Second pass: Merge the multiple defined blocks
    for _type,blocks in needsMerging.iteritems():
        m = merger.Merger(blocks)
        m.merge()
        completeMetadata.update({
          _type: m.block,
        })
    
    #Finally, we have a complete record
    r['metadata'] = completeMetadata
    completeRecords.append(e.finalPassEnforceSchema(r))
    return completeRecords


def delete_by_bibcode(bibcode):
    with session_scope() as session:
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is not None:
            session.delete(r)
            session.commit()


def update_storage(bibcode, type, payload):
    if not isinstance(payload, basestring):
        payload = json.dumps(payload)
    with session_scope() as session:
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is None:
            r = Records(bibcode=bibcode)
            session.add(r)
        now = get_date()
        if type == 'metadata' or type == 'bib_data':
            r.bib_data = payload
            r.bib_data_updated = now 
        elif type == 'nonbib_data':
            r.nonbib_data = payload
            r.nonbib_data_updated = now
        elif type == 'orcid_claims':
            r.orcid_claims = payload
            r.orcid_claims_updated = now
        elif type == 'fulltext':
            r.fulltext = payload
            r.fulltext_updated = now
        else:
            raise Exception('Unknown type: %s' % type)
        r.updated = now
        
        session.commit()


def get_record(bibcode):
    with session_scope() as session:
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is None:
            return None
        return r.toJSON()


def update_processed_timestamp(bibcode):
    with session_scope() as session:
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is None:
            raise Exception('Cant find bibcode {0} to update timestamp'.format(bibcode))
        r.processed = get_date()
        session.commit()