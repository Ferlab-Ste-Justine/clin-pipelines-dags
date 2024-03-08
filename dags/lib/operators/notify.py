from lib.operators.pipeline import PipelineOperator


class NotifyOperator(PipelineOperator):
    def __init__(
            self,
            batch_id: str,
            color: str,
            skip: bool = False,
            **kwargs
    ) -> None:
        super().__init__(
            color=color,
            skip=skip,
            **kwargs
        )
        self.arguments = ['bio.ferlab.clin.etl.LDMNotifier', batch_id]
