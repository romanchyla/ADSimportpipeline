from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.utils.log import get_task_logger
from celery import Task
from aip import error_handler, db
from aip.libs import solr_updater, update_records, utils, read_records
import traceback
from kombu import Exchange, Queue
import math



logger = get_task_logger('ADSimportpipeline')
conf = utils.load_config()
app = Celery('ADSimportpipeline',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['aip.app'])


exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'import-pipeline'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('delete-documents', exch, routing_key='delete-documents'),
    Queue('find-new-records', exch, routing_key='find-new-records'),
    Queue('merge-metadata', exch, routing_key='merge-metadata'),
    Queue('update-record', exch, routing_key='update-record'),
    Queue('update-solr', exch, routing_key='update-solr'),
)

app.conf.update(conf)
db.init_app()

class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(base=MyTask, queue='foo')
def task_foo(a, b):
    print a, b, a+b
    return a+b
        


@app.task(base=MyTask, queue='read-records')
def task_read_records(fingerprints):
    """
    Read ADS records from disk; and inserts them into the queue that ingests them.
    
    @param bibcodes: [(bibcode, json_fingerprint),.....]
    """
    results = read_records.readRecordsFromADSExports(fingerprints)
    if results:
        for r in results:
            task_merge_metadata.delay(r)


@app.task(base=MyTask, queue='merge-metadata')
def task_merge_metadata(record):
    """Receives the full metadata record (incl all the versions) as read by the ADS 
    extractors. We'll merge the versions and create a close-to-canonical version of
    a metadata record."""
    
    result = update_records.mergeRecords([record])
    
    if result and len(result) > 0:
        for r in result: # TODO: save the mid-cycle representation of the metadata ???
            r = solr_updater.SolrAdapter.adapt(r)
            solr_updater.SolrAdapter.validate(r)  # Raises AssertionError if not validated
            task_update_record.delay(r['bibcode'], 'metadata', r)


@app.task(base=MyTask, queue='update-record')
def task_update_record(bibcode, type, payload):
    """Receives the canonical version of the metadata.
    
    @param record: JSON metadata
    """
    if type not in ('metadata', 'orcid_claims', 'nonbib_data', 'fulltext'):
        raise Exception('Unkwnown type {0} submitted for update'.format(type))
    
    # save into a database
    update_records.update_storage(bibcode, type, payload)
    task_update_solr.delay(bibcode)


@app.task(base=MyTask, queue='update-solr')
def task_update_solr(bibcode, force=False, delayed=1):
    """Receives bibcodes and checks the database if we have all the 
    necessary pieces to push to solr. If not, then postpone and 
    push later.
    
    """
    #check if we have complete record
    r = update_records.get_record(bibcode)
    if r is None:
        raise Exception('The bibcode {0} doesn\'t exist!'.format(bibcode))
    
    bib_data_updated = r.get('bib_data_updated', None) 
    orcid_claims_updated = r.get('orcid_claims_updated', None)
    nonbib_data_updated = r.get('nonbib_data_updated', None)
    fulltext_updated = r.get('fulltext_updated', None)
    processed = r.get('processed', utils.get_date('1800'))
     
    is_complete = all(bib_data_updated, orcid_claims_updated, nonbib_data_updated)
    
    # check if we need any to do any updating
    if is_complete and force is False:
        if all(bib_data_updated and bib_data_updated < processed,
               orcid_claims_updated and orcid_claims_updated < processed,
               nonbib_data_updated and nonbib_data_updated < processed):
            return # nothing to do, it wasn already indexed/processed 
    else:
        # if not complete, register a delayed execution
        c = min(app.conf.get('MAX_DELAY', 24*3600*2), # two days 
                            math.pow(app.conf.get('DELAY_BASE', 10), delayed))
        logger.warn('{bibcode} is not yet complete, registering delayed execution in {time}s'.format(
                        bibcode=bibcode, time=c))
        task_update_solr.apply_async((bibcode, delayed+1), countdown = c)
        return
        
    # build the record and send it to solr
    solr_updater.update_solr(r, app.conf.get('SOLR_URLS'))
    update_records.update_processed_timestamp(bibcode)


@app.task(base=MyTask, queue='find-new-records')
def task_find_new_records(fingerprints):
    """Finds bibcodes that are in need of updating. It will do so by comparing
    the json_fingerprint against the new record.
    
    @param fingerprints: [(bibcode, json_fingerprint),....]
    
    TODO: likely an obsolete method, to be removed/nuked/erased?
    """
    
    results = [] #_mongo.findNewRecords(fingerprints)
    if results:
        task_read_records.delay(results)
        
        
@app.task(base=MyTask, queue='delete-documents')
def task_delete_documents(bibcodes):
    """Delete documents from SOLR and from our storage.
    
    @param bibcodes: array of bibcodes 
    """
    for b in bibcodes:
        update_records.delete_by_bibcode(b)
    deleted, failed = solr_updater.delete_by_bibcodes(bibcodes, app.conf['SOLR_URLS'])
    task_handle_errors.delay('ads.import-pipeline.delete-documents', failed)


@app.task(base=MyTask, queue='errors')
def task_handle_errors(producer, message, redelivered=False):
    """
    @param producer: string, the name of the queue where the error originated
    @param body: 
    """
    
    if redelivered:
        logger.error("Fail: %s" % message)
        return
  
    #Iterate over each element of the batch, log and discard the failure(s)
    for content in message:
        try:
            logger.info('Trying to recover: %s' % producer)
            error_handler.handle(producer, content)
        except Exception, e:
            logger.error('%s: %s' % (content, traceback.format_exc()))
    





if __name__ == '__main__':
    app.start()