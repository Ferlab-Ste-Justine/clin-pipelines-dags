from airflow.decorators import task_group

from lib.tasks import es as es_tasks
from lib.utils_es import format_es_url


@task_group(group_id='es')
def es():
    es_tasks.test_duplicated_by_url \
        .override(task_id='es_test_duplicated_variant')(url=format_es_url('variant'))

    es_tasks.test_duplicated_by_url \
        .override(task_id='es_test_duplicated_cnv')(url=format_es_url('cnv'))

    es_tasks.test_disk_usage()
