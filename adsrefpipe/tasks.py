
from __future__ import absolute_import, unicode_literals
from adsrefpipe import app as app_module
from kombu import Queue

from adsrefpipe.utils import read_reference_text_file, resolve_text_references

import os

# TODO first off grab the backend url from config and also see how does celery init this
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), '../')),
                                            backend='redis://localhost:6379/0')

app.conf.CELERY_QUEUES = (
    Queue('process', app.exchange, routing_key='process'),
)

logger = app.logger

POPULATE_COMPARE = True

@app.task(queue='process')
def task_process_reference_file(source_file):
    """

    :param source_file: source filename
    :return:
    """
    # read from source file the bibcode and references
    bibcode, references = read_reference_text_file(source_file)
    # send references to reference_service
    if bibcode and len(references) > 0:
        resolved = resolve_text_references(references)
        # if resolved successfully populate tables
        if resolved:
            # also write to compare table if the flag is set
            classic_resolved_filename = source_file.replace('sources', 'resolved') + '.result' if POPULATE_COMPARE else None
            result = app.populate_tables(bibcode, source_file, resolved, classic_resolved_filename)
            return result
    return False
