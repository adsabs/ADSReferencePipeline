from adsrefpipe import app as app_module
from kombu import Queue

import os
import pickle

import adsrefpipe.utils as utils

from adsputils import load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=proj_home,
                                            local_config=globals().get('local_config', {}),
                                            backend=config.get('REDIS_BACKEND'))

app.conf.CELERY_QUEUES = (
    Queue('task_process_reference', app.exchange, routing_key='task_process_reference'),
)

logger = app.logger

@app.task(queue='task_process_reference')
def task_process_reference(reference_task):
    """

    :param reference_task:
    :return:
    """
    try:
        resolved = utils.get_resolved_references(reference_task['reference'], reference_task['resolver_service_url'])
        if not resolved:
            return False

        # TODO: remove comparing to classic before going to production
        classic_resolved_filename = reference_task['source_filename'].replace('sources', 'resolved') + '.result' if config['COMPARE_CLASSIC'] else None

        status = app.populate_tables_post_resolved(resolved, reference_task['source_bibcode'], classic_resolved_filename)
        if not status:
            return False

        return True
    except KeyError:
        return False
