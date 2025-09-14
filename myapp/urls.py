from django.urls import path
from myapp.views import products_stat_view


pp_name = 'myapp'  #чтобы файл был действительно отдельным пространством имен


urlpatterns = [
    path("api/products-stat/", products_stat_view, name="products-stat"),
]