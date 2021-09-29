from adsrefpipe import app as app_module
from kombu import Queue

import os

import adsrefpipe.utils as utils

from adsputils import load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=proj_home,
                                            local_config=globals().get('local_config', {}),
                                            backend=config.get('REDIS_BACKEND'))

app.conf.CELERY_QUEUES = (
    Queue('process_references', app.exchange, routing_key='process_references'),
    Queue('reprocess_references', app.exchange, routing_key='reprocess_references')
)

logger = app.logger

def task_process(results, filename, parsername):
    """

    :param results:
    :param filename:
    :param parsername:
    :return:
    """
    num_references = config['REFERENCE_PIPELINE_MAX_NUM_REFERENCES']
    for result in results:
        references = result['references']
        # save the initial records in the database,
        # this is going to be useful since it allows us to be able to tell if
        # anything went wrong with the service that we did not get back the results
        references = app.populate_tables_pre_resolved_initial_status(source_bibcode=result['bibcode'],
                                                                     source_filename=filename,
                                                                     parsername=parsername,
                                                                     references=references)
        if not references:
            return False
        resolver_service_url = config['REFERENCE_PIPELINE_SERVICE_URL'] + app.get_reference_service_endpoint(parsername)
        resolved_references = []
        for batch in range(0, len(references), num_references):
            # send references to reference_service one batch at a time
            references_batch = references[batch: batch+num_references]
            # accumulate them to be inserted to db in once chunk
            resolved_batch = utils.get_resolved_references(references_batch, resolver_service_url)
            if not resolved_batch:
                return False
            resolved_references += resolved_batch
        classic_resolved_filename = filename.replace('sources', 'resolved') + '.result' if config['COMPARE_CLASSIC'] else None
        status = app.populate_tables_post_resolved(resolved_references, result['bibcode'], classic_resolved_filename)
        if not status:
            return False
    return True

@app.task(queue='process_references')
def task_process_references(toREFs):
    """

    :param toREFs: one of the toREFs derived classes (ie, ELSEVIERtoREFs, ARXIVtoREFs)
            has a process_and_dispatch method to return references that were cleaned and parsed
    :return:
    """
    results = toREFs.process_and_dispatch()
    if not results:
        logger.error("Unable to parse %s." % toREFs.filename)
        return False
    return task_process(results, toREFs.filename, toREFs.parsername)

@app.task(queue='reprocess_references')
def task_reprocess_references(toREFs):
    """

    :param toREFs: one of the toREFs derived classes (ie, ELSEVIERtoREFs, ARXIVtoREFs)
            has a dispatch method to return references that were parsed
    :return:
    """
    results = toREFs.dispatch()
    if not results:
        logger.error("Unable to parse %s." % toREFs.filename)
        return False
    return task_process(results, toREFs.filename, toREFs.parsername)


