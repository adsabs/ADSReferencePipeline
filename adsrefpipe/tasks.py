from adsrefpipe import app as app_module
from kombu import Queue

import os

from adsputils import load_config
config = {}
config.update(load_config())

from adsrefpipe.utils import read_reference_text_file, resolve_references, ReferenceType, ReprocessQueryType
from adsrefpipe.xmlparsers.handler import verify


app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), '../')),
                                            local_config=globals().get('local_config', {}),
                                            backend=config['REDIS_BACKEND'])

app.conf.CELERY_QUEUES = (
    Queue('process_references', app.exchange, routing_key='process_references'),
    Queue('reprocess_subset_references', app.exchange, routing_key='reprocess_subset_references')
)

logger = app.logger

POPULATE_COMPARE = True

@app.task(queue='process_references')
def task_process_reference_file(reference_filename):
    """

    :param reference_filename:
    :return:
    """
    # first figure out which parser to call
    name = app.get_parser_name(reference_filename)

    # it is a text file
    if name == 'Text':
        type = ReferenceType.text
        # read from source file the bibcode and references
        # it shall be parsed on the side of service, so nothing more to do
        bibcode, references = read_reference_text_file(reference_filename)
        if bibcode and len(references) > 0:
            # have it in the same sturcture as xml so that the same code can be applied
            results = [{'bibcode': bibcode, 'references': references}]
        else:
            logger.error("Unable to parse %s." %reference_filename)
            return False

    # it is an xml file
    else:
        try:
            type = ReferenceType.xml
            # figure out which xml parser to call
            parser = verify(name)
            # read from source file the bibcode and references already tagged
            results = parser(reference_filename)
        except:
            logger.error("Unable to parse %s." %reference_filename)
            return False

    num_references = config['REFERENCE_PIPELINE_MAX_NUM_REFERENCES']
    for result in results:
        references = result['references']
        # save the initial records in the database,
        # this is going to be useful since it allows us to be able to tell if
        # anything went wrong with the service that we did not get back the results
        status, references = app.populate_tables_new_precede(type, result['bibcode'], reference_filename, name, references)
        if not status:
            return False
        resolved = []
        for batch in range(0, len(references), num_references):
            # send references to reference_service one batch at a time
            references_batch = references[batch: batch+num_references]
            # accumulate them to be inserted to db in once chunk
            resolved_batch = resolve_references(type, references_batch)
            if not resolved_batch:
                return False
            resolved += resolved_batch
        classic_resolved_filename = reference_filename.replace('sources', 'resolved') + '.result' if POPULATE_COMPARE else None
        status = app.populate_tables_succeed(resolved, result['bibcode'], classic_resolved_filename)
        if not status:
            return False
    return True

@app.task(queue='reprocess_subset_references')
def task_reprocess_subset_references(record):
    """

    :param records:
    :return:
    """
    reference_filename = record['source_filename']
    type = ReferenceType.text if record['parser'] == 'Text' else ReferenceType.xml
    status, references = app.populate_tables_retry_precede(type, record['source_bibcode'], reference_filename,
                                                           record['source_modified'], record['references'])
    if not status:
        return False
    resolved = []

    num_references = config['REFERENCE_PIPELINE_MAX_NUM_REFERENCES']
    for batch in range(0, len(references), num_references):
        # send references to reference_service one batch at a time
        references_batch = references[batch: batch+num_references]
        # accumulate them to be inserted to db in once chunk
        resolved += resolve_references(type, references_batch)
        if not resolved:
            return False
    classic_resolved_filename = reference_filename.replace('sources', 'resolved') + '.result' if POPULATE_COMPARE else None
    return app.populate_tables_succeed(resolved, record['source_bibcode'], classic_resolved_filename)


