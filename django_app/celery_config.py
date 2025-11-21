from celery import Celery
import os
from django.utils import timezone


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_app.settings')

app = Celery('rencon')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
app.now = timezone.now


# Использование DatabaseScheduler для хранения расписания в базе данных
app.conf.beat_scheduler = 'django_celery_beat.schedulers:DatabaseScheduler'