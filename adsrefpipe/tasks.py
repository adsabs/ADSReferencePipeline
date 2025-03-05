from adsrefpipe import app as app_module
from kombu import Queue

import os

import adsrefpipe.utils as utils

from adsputils import load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=proj_home,
                                            local_config=globals().get('local_config', {}))

app.conf.CELERY_QUEUES = (
    Queue('task_process_reference', app.exchange, routing_key='task_process_reference'),
)

logger = app.logger


class FailedRequest(Exception):
    """
    Failed to connect to reference service.
    """
    pass


@app.task(queue='task_process_reference', max_retries=config['MAX_QUEUE_RETRIES'])
def task_process_reference(reference_task: dict) -> bool:
    """
    process a reference task by resolving references and updating the database

    :param reference_task: dictionary containing reference details and service url
    :return: True if processing is successful, False otherwise
    """
    try:
        resolved = utils.post_request_resolved_reference(reference_task['reference'], reference_task['resolver_service_url'])
        # if failed to connect to reference service, raise a exception to requeue, for max_retries times
        if not resolved:
            raise FailedRequest

        # TODO: remove comparing to classic before going to production
        classic_resolved_filename = reference_task['source_filename'].replace('sources', 'resolved') + '.result' if config['COMPARE_CLASSIC'] else None

        status = app.populate_tables_post_resolved(resolved, reference_task['source_bibcode'], classic_resolved_filename)
        if not status:
            return False

        return True

    except KeyError:
        return False

# dont know how to unittest this part
# this (app.start()) the only line that is not unittested
# and since i want all modules to be 100% covered,
# making this line not be considered part of coverage
if __name__ == '__main__':    # pragma: no cover
    app.start()
