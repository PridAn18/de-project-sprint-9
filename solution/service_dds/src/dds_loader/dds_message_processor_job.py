import uuid
from logging import Logger
from datetime import datetime, timezone

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size



    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.now(timezone.utc)}: Message received")


            order = msg['payload']
            user = order['user']
            user_id = user['id']
            username = user['name']
            userlogin = user['login']
            load_dt = datetime.now()
            order_cost = str(order['cost'])
            order_payment = str(order['payment'])
            order_status = order['status']
            restaurant_id = order['restaurant']['id']
            restaurant_name = order['restaurant']['name']
            order_id = str(order['id'])
            order_dt = order['date']

            h_user_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, user_id))
            h_order_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, order_id))
            h_restaurant_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, restaurant_id))

            hk_order_user_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, h_user_pk])))
            hk_user_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_user_pk, username, userlogin])))
            hk_restaurant_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_restaurant_pk, restaurant_name])))
            
            hk_order_status_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, order_status])))
            hk_order_cost_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, order_cost, order_payment])))

            self._dds_repository.h_user_insert(h_user_pk, user_id, load_dt)
            self._dds_repository.h_restaurant_insert(h_restaurant_pk, restaurant_id, load_dt)
            self._dds_repository.h_order_insert(h_order_pk, order_id, order_dt, load_dt )
            self._dds_repository.l_order_user_insert(hk_order_user_pk, h_order_pk, h_user_pk, load_dt )
            self._dds_repository.s_user_names_insert(hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt)
            self._dds_repository.s_restaurant_names_insert(hk_restaurant_names_hashdiff, h_restaurant_pk, restaurant_name, load_dt)
            self._dds_repository.s_order_cost_insert(hk_order_cost_hashdiff, h_order_pk, order_cost, order_payment, load_dt)
            self._dds_repository.s_order_status_insert(hk_order_status_hashdiff, h_order_pk, order_status, load_dt)



            # Обрабатываем каждый продукт в заказе
            for product in order["products"]:
                # Создаем UUID

                product_id = product['id']
                category_name = product['category']
                product_name = product['name']

                h_product_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, product_id))
                h_category_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, category_name))

                
                
                
                hk_order_product_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, h_product_pk])))
                hk_product_category_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, h_category_pk])))
                hk_product_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, product_name])))
                hk_product_restaurant_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, h_restaurant_pk])))

                self._dds_repository.h_product_insert(h_product_pk, product_id, load_dt)
                self._dds_repository.h_category_insert(h_category_pk, category_name, load_dt)

                self._dds_repository.l_order_product_insert(hk_order_product_pk, h_order_pk, h_product_pk, load_dt)
                self._dds_repository.l_product_restaurant_insert(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt)
                self._dds_repository.l_product_category_insert(hk_product_category_pk, h_product_pk, h_category_pk, load_dt)

                self._dds_repository.s_product_names_insert(hk_product_names_hashdiff, h_product_pk, product_name, load_dt)

                # 4.6. Создаем сообщение для Kafka для каждого продукта
                dst_msg = {
                    "order_id": order_id, 
                    "user_uuid": h_user_pk,
                    "product": {
                        "product_uuid": h_product_pk,
                        "product_name": product["name"],
                        "category_uuid": h_category_pk,
                        "category_name": product["category"]
                    }
                }
                # 4.7. Отправляем сообщение в Kafka
                self._producer.produce(dst_msg)
                self._logger.info(f"{datetime.now(timezone.utc)}. Message Sent")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")

    
    