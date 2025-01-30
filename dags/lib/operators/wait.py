from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator


class WaitOperator(BashOperator):

    template_fields = BashOperator.template_fields + (
        'skip',
    )

    def __init__(
        self,
        time: str,
        skip: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            bash_command=f'sleep {time}',
            **kwargs,
        )
        self.skip = skip
    
    def execute(self, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        super().execute(**kwargs)
