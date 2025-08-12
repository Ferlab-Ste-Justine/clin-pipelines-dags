import logging
from typing import List

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.context import Context
from lib.config import K8sContext, env
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

    template_fields = SparkOperator.template_fields + ('batch_id', 'analysis_ids', 'steps',)

    def __init__(self,
                 steps: str,
                 app_name: str,
                 spark_class: str,
                 spark_config: str,
                 entrypoint: str = '',
                 batch_id: str = '',
                 analysis_ids: str = '',
                 chromosome: str = '',
                 target_batch_types: List[ClinAnalysis] = None,
                 detect_batch_type_task_id: str = 'detect_batch_type',
                 spark_jar: str = '',
                 skip: str = '',
                 batch_id_deprecated: bool = False,
                 **kwargs
                 ) -> None:
        super().__init__(
            k8s_context=K8sContext.ETL,
            spark_class=spark_class,
            spark_config=spark_config,
            spark_jar=spark_jar,
            skip=skip,
            **kwargs)

        self.steps = steps
        self.app_name = app_name
        self.entrypoint = entrypoint
        self.batch_id = batch_id
        self.batch_id_deprecated = batch_id_deprecated
        self.analysis_ids = analysis_ids
        self.chromosome = chromosome
        self.target_batch_types = [target.value for target in (target_batch_types or [])]
        self.detect_batch_type_task_id = detect_batch_type_task_id

    def execute(self, context: Context):
        
        if env in self.skip_env:
            raise AirflowSkipException()

        if self.skip:
            raise AirflowSkipException()

        # Check if batch type is in target batch types if batch_id and target_batch_types is defined
        # Useful for dynamically mapped task for that should only be run for specific batch types
        if self.target_batch_types:
            
            batch_type_by_batch_id = context['ti'].xcom_pull(task_ids=self.detect_batch_type_task_id, key=self.batch_id) if self.batch_id and len(self.batch_id) > 0 else None
            batch_type_by_analysis_id = context['ti'].xcom_pull(task_ids=self.detect_batch_type_task_id, key=self.analysis_ids[0]) if self.analysis_ids and len(self.analysis_ids) > 0 else None
                     
            target_batch_type_message = f'Batch id \'{self.batch_id}\' of batch type \'{batch_type_by_batch_id}\' or Analysis IDs \'{self.analysis_ids}\' of batch type \'{batch_type_by_analysis_id}\' least one expected to be in ' \
                                            f'target batch types: {self.target_batch_types}'
            
            if batch_type_by_batch_id and batch_type_by_analysis_id and batch_type_by_batch_id != batch_type_by_analysis_id:
                raise AirflowFailException(target_batch_type_message)
                      
            if (batch_type_by_batch_id and batch_type_by_batch_id not in self.target_batch_types) or (batch_type_by_analysis_id and batch_type_by_analysis_id not in self.target_batch_types):
                raise AirflowSkipException(target_batch_type_message)
            
            logging.info(target_batch_type_message)

        arguments = build_etl_job_arguments(
            app_name=self.app_name,
            entrypoint=self.entrypoint,
            steps=self.steps,
            batch_id=self.batch_id,
            batch_id_deprecated=self.batch_id_deprecated,
            analysis_ids=self.analysis_ids,
            chromosome=self.chromosome
        )

        self.arguments = arguments

        logging.info('Arguments for Spark job: %s', self.arguments)

        super().execute(context)
