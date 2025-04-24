from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from lib.config import K8sContext
from lib.operators.spark import SparkOperator
from lib.utils_etl import ClinAnalysis, build_etl_job_arguments


class SparkETLOperator(SparkOperator):
    """
     A custom Airflow operator for running Spark ETL jobs, extending `SparkOperator`.

     This operator simplifies task creation for ETL jobs by allowing optional batch ID handling
     and validation against target batch types. If a `batch_id` and a `target_batch_types` is provided, the batch type
     is checked against `target_batch_types` before job execution.

     Parameters:
         steps (str): Run type of the ETL. Usually `default` or `initial`.
         app_name (str): Name of the Spark application.
         spark_class (str): Main Spark class.
         spark_config (str): Spark configuration.
         entrypoint (str, optional): Optional entrypoint of the main Spark class. Defaults to ''.
         batch_id (str, optional): Optional batch ID to process. Defaults to ''.
         target_batch_types (List[ClinAnalysis], optional): Allowed batch types for validation.
         detect_batch_type_task_id (str, optional): Task ID of the detect batch type task. Defaults to 'detect_batch_type'.
         spark_jar (str, optional): Path to the Spark JAR. Defaults to the path defined at `config.spark_jar`.
         skip (str, optional): Conditions to skip execution. Defaults to '' which is evaluated to False.
         **kwargs: Additional arguments for `SparkOperator`.
     """

    template_fields = SparkOperator.template_fields + ('batch_id', 'sequencing_ids')

    def __init__(self,
                 steps: str,
                 app_name: str,
                 spark_class: str,
                 spark_config: str,
                 entrypoint: str = '',
                 batch_id: str = '',
                 sequencing_ids: List[str] = None,
                 chromosome: str = '',
                 target_batch_types: List[ClinAnalysis] = None,
                 detect_batch_type_task_id: str = 'detect_batch_type',
                 spark_jar: str = '',
                 skip: str = '',
                 **kwargs
                 ) -> None:
        super().__init__(
            k8s_context=K8sContext.ETL,
            spark_class=spark_class,
            spark_config=spark_config,
            spark_jar=spark_jar,
            skip=skip,
            **kwargs)

        arguments = build_etl_job_arguments(
            app_name=app_name,
            entrypoint=entrypoint,
            steps=steps,
            batch_id=batch_id,
            chromosome=chromosome
        )
        if entrypoint:
            arguments = [entrypoint] + arguments
        if batch_id:
            arguments = arguments + ['--batchId', batch_id]
        if sequencing_ids:
            arguments = arguments + ['--sequencing_ids', sequencing_ids]
        if chromosome:
            arguments = arguments + ['--chromosome', f'chr{chromosome}']

        self.arguments = arguments
        self.batch_id = batch_id
        self.sequencing_ids = sequencing_ids
        self.chromosome = chromosome
        self.target_batch_types = [target.value for target in (target_batch_types or [])]
        self.detect_batch_type_task_id = detect_batch_type_task_id

    def execute(self, context: Context):
        # Check if batch type is in target batch types if batch_id and target_batch_types is defined
        # Useful for dynamically mapped task for that should only be run for specific batch types
        if self.target_batch_types:
            key = self.batch_id if self.batch_id else self.sequencing_ids
            batch_type = context['ti'].xcom_pull(task_ids=self.detect_batch_type_task_id, key=key)[0]
            if batch_type not in self.target_batch_types:
                raise AirflowSkipException(f'Batch or Sequencing ids \'{key}\' of batch type \'{batch_type}\' is not in '
                                           f'target batch types: {self.target_batch_types}')

        super().execute(context)
