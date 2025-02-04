import os
import boto3
import ydb
import ydb.iam
from io import StringIO
from botocore.config import Config

# Чтение переменных окружения
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
YDB_ENDPOINT = os.getenv('YDB_ENDPOINT')
YDB_DATABASE = os.getenv('YDB_DATABASE')

def get_s3_instance():
    """Создание клиента S3 для работы с Яндекс Облаком."""
    try:
        session = boto3.session.Session()
        return session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='ru-central1',
            config=Config(
                signature_version='s3v4',
                s3={'payload_signing_enabled': False}  # Отключаем подпись содержимого
            )
        )
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        raise

def upload_dump_to_s3(key, buffer):
    """Загрузка данных из буфера в S3."""
    print("\U0001F4C2 Starting upload to Object Storage")
    try:
        get_s3_instance().put_object(
            Bucket=BUCKET_NAME,
            Key=f"quote-{key}.txt",
            Body=buffer.getvalue()  # Загружаем содержимое буфера
        )
        print("\U0001f680 Uploaded")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise

def record_ten_quote(session):
    """Запись десяти цитат из YDB в S3."""
    yql = "SELECT * FROM Quotes WHERE id <= 10;"
    
    result = session.transaction().execute(
        yql,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )

    for row in result[0].rows:
        quote = f"{row.quote} {row.author}"
        
        # Используем StringIO для создания буфера в памяти
        buffer = StringIO()
        buffer.write(quote)
        buffer.seek(0)  # Сброс позиции чтения

        upload_dump_to_s3(str(row.id), buffer)

    return "Ten quotes are recorded to the object storage"

def handler(event, context):
    """Обработчик AWS Lambda."""
    # Создаем драйвер YDB
    driver = ydb.Driver(
        endpoint=YDB_ENDPOINT,
        database=YDB_DATABASE,
        credentials=ydb.iam.MetadataUrlCredentials(),
    )

    # Ждем, пока драйвер станет активным для запросов.
    driver.wait(fail_fast=True, timeout=5)

    # Создаем пул сессий для управления сессиями YDB.
    pool = ydb.SessionPool(driver)

    try:
        text_for_message = str(pool.retry_operation_sync(record_ten_quote))
        r = {'statusCode': 200, 'body': text_for_message}
    except Exception as e:
        r = {'statusCode': 404, 'body': str(e)}

    return r
