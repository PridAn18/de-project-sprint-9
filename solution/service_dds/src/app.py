import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.repository.dds_repository import DdsRepository
from dds_loader.dds_message_processor_job import DdsMessageProcessor

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
    dds_repository = DdsRepository(config.pg_warehouse_db())
    # Инициализируем процессор сообщений.
    proc = DdsMessageProcessor(
        consumer=kafka_consumer,
        producer=kafka_producer,
        dds_repository=dds_repository,
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
