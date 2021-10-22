from abc import ABC
from abc import abstractmethod


class BaseMonitor(ABC):
    def __init(self, **kwargs):
        pass

    @abstractmethod
    def post_status(
        self,
        msg_type: str,
        msg_payload: str,
    ) -> bool:
        pass
