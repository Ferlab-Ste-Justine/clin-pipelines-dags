from lib.config import K8sContext, clin_import_bucket
from lib.operators.pipeline import PipelineOperator


class PipelineFhirImportOperator(PipelineOperator):
    template_fields = PipelineOperator.template_fields + ('batch_id',)

    def __init__(self,
                 color: str,
                 batch_id: str,
                 main_class: str,
                 dry_run: bool = False,
                 full_metadata: bool = True,
                 skip: bool = False,
                 **kwargs) -> None:
        super().__init__(
            name='etl-ingest-fhir-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=clin_import_bucket,
            color=color,
            skip=skip,
            **kwargs,
        )
        self.batch_id = batch_id
        self.arguments = [
            main_class,
            self.batch_id,
            str(dry_run).lower(),
            str(full_metadata).lower()
        ]
