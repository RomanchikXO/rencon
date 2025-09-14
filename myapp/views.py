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

@csrf_exempt  # отключаем CSRF для API
def products_stat_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed, use POST"}, status=405)

    try:
        data = json.loads(request.body)

        # Проверяем обязательное поле inn
        if "inn" not in data:
            return JsonResponse({"error": "Missing field 'inn'"}, status=400)

        inn = int(data["inn"])
        date_from = parse_date(data.get("date_from"))  # yyyy-mm-dd
        date_to = parse_date(data.get("date_to"))      # yyyy-mm-dd
        articles = data.get("articles")  # список артикулов или None

        # Получаем ЛК по inn
        lk = WbLk.objects.filter(inn=inn).first()
        if not lk:
            return JsonResponse({"error": "WbLk не найден"}, status=404)

        # Фильтруем nmids
        nmids_qs = nmids.objects.filter(lk=lk)
        if articles:
            nmids_qs = nmids_qs.filter(nmid__in=articles)
        nmids_list = list(nmids_qs.values_list("nmid", flat=True))

        # Фильтруем ProductsStat
        stats_qs = ProductsStat.objects.filter(
            nmid__in=nmids_list,
        )
        if date_from:
            stats_qs = stats_qs.filter(date_wb__date__gte=date_from)
        if date_to:
            stats_qs = stats_qs.filter(date_wb__date__lte=date_to)

        stats = list(stats_qs.values())

    except Exception as e:
        logger.error(e)
        return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse(stats, safe=False)
