from __future__ import absolute_import, unicode_literals
from adsrefpipe import app as app_module
from kombu import Queue

import os

from adsputils import load_config
config = {}
config.update(load_config())

from adsrefpipe.utils import read_reference_text_file, resolve_text_references, resolve_xml_references
from adsrefpipe.xmlparsers.handler import verify


# TODO first off grab the backend url from config and also see how does celery init this
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), '../')),
                                            backend='redis://localhost:6379/0')

app.conf.CELERY_QUEUES = (
    Queue('process_text', app.exchange, routing_key='process_text'),
    Queue('process_xml', app.exchange, routing_key='process_xml'),
)

logger = app.logger

POPULATE_COMPARE = True

@app.task(queue='process_text')
def task_process_reference_text_file(source_files):
    """

    :param source_files: source filenames
    :return:
    """
    num_references = config['REFERENCE_PIPELINE_MAX_NUM_REFERENCES']

    for source_file in source_files:
        # read from source file the bibcode and references
        bibcode, references = read_reference_text_file(source_file)
        if bibcode and len(references) > 0:
            for batch in range(0, len(references), num_references):
                # send references to reference_service one batch at a time
                references_batch = references[batch: batch+num_references]
                resolved = resolve_text_references(references_batch)
                if not resolved:
                    return False
                # if resolved successfully continue to populate tables
                # also write to compare table if the flag is set
                classic_resolved_filename = source_file.replace('sources', 'resolved') + '.result' if POPULATE_COMPARE else None
                status = app.populate_tables(bibcode, source_file, resolved, classic_resolved_filename)
                if not status:
                    return False
            return True


@app.task(queue='process_xml')
def task_process_reference_xml_file(source_files):
    """

    :param source_file: source filename
    :return:
    """
    num_references = config['REFERENCE_PIPELINE_MAX_NUM_REFERENCES']

    for source_file in source_files:
        # first figure out which parser to call
        parser = verify(source_file)
        # read from source file the bibcode and references already tagged
        results = parser(source_file)
        for result in results:
            references = result['references']
            for batch in range(0, len(references), num_references):
                references_batch = references[batch: batch+num_references]
                resolved = resolve_xml_references(references_batch)
                if not resolved:
                    return False
                # if resolved successfully continue to populate tables
                # also write to compare table if the flag is set
                classic_resolved_filename = source_file.replace('sources', 'resolved') + '.result' if POPULATE_COMPARE else None
                status = app.populate_tables(result['bibcode'], source_file, resolved, classic_resolved_filename)
                if not status:
                    return False
    return True
