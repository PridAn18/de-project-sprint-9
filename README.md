# Проект за 9 спринт
Проект по облачным технологиям YandexCloud

Проект реализует 3 ETL сервиса в Kubernetes:

- **stg_service**: Читает сырые данные из **Kafka** и **Redis**, объединяет их, отправляет данные в Staging DWH и **Kafka** для обратки следующим сервисом

- **dds_service**: Читает данные из **Kafka**, заполняет DDS слой в DWH, cоздает сообщение для **Kafka** для заполнения CDM слоя

- **cdm_service**: Читает данные из **Kafka**, заполняет CDM слой DWH.

### Дашборд в DataLens
Создаём дашбоард в **DataLens** [ссылка](https://datalens.yandex/3wo87v6i3t0qp)

<img src="solution/img/dashboard.png" alt="Contact Point" width="auto"/>

### Структура репозитория 
Папка solution содержит необходимые файлы для работы проекта.
1. Папка `img` хранит изображения.
2. `docker-compose.yml` файл создания контейнеров для работы проекта.
3. В папке `service_stg` хранятся файлы для работы контейнера и создания образа: 
    * Папке `app` содержит стркутуру helm для запуска приложения в Kubernetes:
      * В папке `templates` шаблон манифеста
      * `Chart.yaml` метаданные Helm-чарта
      * `values.yaml` файл с параметрами для приложения и образом
	* В папке `src` код работы приложения.
    * `dockerfile` файл для создания образа контейнера .
    * `requirements.txt` зависимости для работы сервиса.
3. В папке `service_dds` хранятся файлы для работы контейнера и создания образа: 
    * Папке `app` содержит стркутуру helm для запуска приложения в Kubernetes.
		* В папке `src` код работы приложения.
    * `dockerfile` файл для создания образа контейнера .
    * `requirements.txt` зависимости для работы сервиса.
3. В папке `service_cdm` хранятся файлы для работы контейнера и создания образа: 
    * Папке `app` содержит стркутуру helm для запуска приложения в Kubernetes.
		* В папке `src` код работы приложения.
    * `dockerfile` файл для создания образа контейнера .
    * `requirements.txt` зависимости для работы сервиса.


### Инициализация

Сборка сервисов

```bash
cd путь_до_каталога_с_docker-compose.yml
docker compose up -d --build 
```

Подготовьте kubeconfig: запишите путь до файла kubeconfig в переменную.

```bash
export KUBECONFIG=полный_путь_до_файла_kobeconfig 
```


### Сервисы в реджистри

Для безопасности реджистри скрыт

```yaml
image:
  repository: "cr.yandex/<registry id>/stg_service"
  pullPolicy: IfNotPresent
  # Overrides the image tag whose default is the chart appVersion.
  tag: "version-1.0"
```

```yaml
image:
  repository: "cr.yandex/<registry id>/dds_service"
  pullPolicy: IfNotPresent
  # Overrides the image tag whose default is the chart appVersion.
  tag: "version-1.0"
```

```yaml
image:
  repository: "cr.yandex/<registry id>/cdm_service"
  pullPolicy: IfNotPresent
  # Overrides the image tag whose default is the chart appVersion.
  tag: "version-1.0"
```

