import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.repository.stg_repository import StgRepository
from stg_loader.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)


# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()
    kafka_consumer = config.kafka_consumer()
    kafka_producer = config.kafka_producer()
    redis_client = config.redis_client()
    stg_repository = StgRepository(config.pg_warehouse_db())
    # Инициализируем процессор сообщений.
    proc = StgMessageProcessor(
        consumer=kafka_consumer,
        producer=kafka_producer,
        redis_client=redis_client,
        stg_repository=stg_repository,
        batch_size=100,  # Или любое другое значение по умолчанию
        logger=app.logger
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(SampleMessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
