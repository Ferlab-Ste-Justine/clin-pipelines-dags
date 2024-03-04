from typing import Sequence

from lib.operators.pipeline import PipelineOperator


class NotifyOperator(PipelineOperator):
    template_fields: Sequence[str] = (*PipelineOperator.template_fields, 'batch_id')

    def __init__(
            self,
            batch_id: str,
            color: str,
            skip: bool = False,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.color = color
        self.skip = skip
        self.arguments = ['bio.ferlab.clin.etl.LDMNotifier', batch_id]
