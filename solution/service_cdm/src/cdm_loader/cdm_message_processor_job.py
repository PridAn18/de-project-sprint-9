from logging import Logger
from datetime import datetime, timezone

from lib.kafka_connect import KafkaConsumer

from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size



    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START cdm")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.now(timezone.utc)}: Message received cdm")


            self._cdm_repository.user_category_counters_insert(
                msg["user_uuid"],
                msg["product"]["category_uuid"],
                msg["product"]["category_name"])
            
            self._cdm_repository.user_product_counters_insert(
                msg["user_uuid"],
                msg["product"]["product_uuid"],
                msg["product"]["product_name"])

            
            
            self._logger.info(f"{datetime.now(timezone.utc)}. Message Sent cdm")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH cdm")

    
    