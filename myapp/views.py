# myapp/views.py
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.dateparse import parse_date
from django.views import View
from myapp.models import WbLk, nmids, ProductsStat
from context_logger import ContextLogger
import logging
import json

logger = ContextLogger(logging.getLogger("myapp"))

