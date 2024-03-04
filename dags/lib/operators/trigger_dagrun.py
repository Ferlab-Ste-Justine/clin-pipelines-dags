from typing import Sequence

from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator as AirflowTriggerDagRunOperator
from airflow.utils.context import Context


class TriggerDagRunOperator(AirflowTriggerDagRunOperator):
    template_fields: Sequence[str] = (*AirflowTriggerDagRunOperator.template_fields, 'skip')

    def __init__(
            self,
            skip: bool = False,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.skip = skip

    def execute(self, context: Context):
        if self.skip:
            raise AirflowSkipException()

        super().execute(context)
