from django.contrib import admin
from .models import (WbLk, Price, CeleryLog, nmids, Stocks, Orders,
                     ProductsStat, Supplies, Betweenwarhouses, AreaWarehouses, AdvStat, Adverts,
                     FinData, SaveData, RegionSales)


class AdvertsAdmin(admin.ModelAdmin):
    list_display = ('advert_id', 'type_adv', 'status')
    list_filter = ('type_adv', 'status')
    search_fields = ('advert_id',)


class AdvStatAdmin(admin.ModelAdmin):
    list_display = ('nmid', 'date_wb', 'app_type', 'advert_id')
    search_fields = ('nmid', 'advert_id')
    ordering = ('-date_wb',)
    list_filter = ('nmid', 'advert_id', 'date_wb',)


class AreaWarehousesAdmin(admin.ModelAdmin):
    list_display = ('area', 'warehouses')
    search_fields = ('area',)
    ordering = ('area',)


class BetweenwarhousesAdmin(admin.ModelAdmin):
    list_display = ('nmid', 'incomeid', 'warehousename')


class SuppliesAdmin(admin.ModelAdmin):
    list_display = ('incomeId', 'nmid', 'warehouseName', 'dateClose')
    search_fields = ('incomeId', 'nmid')
    ordering = ('-dateClose',)
    list_filter = ('incomeId', 'dateClose', 'warehouseName')


class ProductsStatAdmin(admin.ModelAdmin):
    list_display = ('nmid', 'date_wb', 'buyoutPercent')
    search_fields = ('nmid',)
    ordering = ('-date_wb',)  # Сортировка по умолчанию
    list_filter = ('date_wb',)


class PriceAdmin(admin.ModelAdmin):
    list_display = ('get_lk_name', 'nmid', 'vendorcode', 'updated_at', 'spp', 'blackprice')  # Определяет, какие поля будут отображаться в списке
    search_fields = ('lk__name', 'nmid', 'vendorcode') # Поля для поиска
    ordering = ('updated_at',)  # Сортировка по умолчанию
    list_filter = ('lk',)  # Фильтр по полю 'lk'

    def get_lk_name(self, obj):
        return obj.lk.name

    get_lk_name.short_description = 'Личный кабинет'

class CeleryLogAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'source', 'level', 'message')
    list_filter = ('level', 'source', 'timestamp')
    search_fields = ('message',)

class NmidsAdmin(admin.ModelAdmin):
    list_display = (
        'nmid', 'title', 'brand', 'vendorcode', 'subjectname', 'needkiz',
        'lk', 'created_at', 'updated_at', 'added_db'
    )
    list_filter = ('brand', 'subjectname', 'needkiz', 'lk')
    search_fields = ('nmid', 'vendorcode', 'title', 'brand', 'nmuuid')
    ordering = ('-added_db',)
    date_hierarchy = 'added_db'

class StocksAdmin(admin.ModelAdmin):
    list_display = (
        'supplierarticle', 'nmid', 'barcode',
        'quantity', 'inwaytoclient', 'inwayfromclient',
        'quantityfull', 'warehousename', 'lastchangedate',
        'isrealization',
    )
    list_filter = ('warehousename', 'issupply', 'isrealization')
    search_fields = ('supplierarticle', 'barcode', 'nmid')
    ordering = ('-lastchangedate',)

class OrdersAdmin(admin.ModelAdmin):
    list_display = (
        'date', 'lastchangedate', 'supplierarticle',
        'nmid', 'barcode', 'warehousename', 'countryname',
        'brand', 'totalprice', 'finishedprice', 'iscancel'
    )
    list_filter = ('lk', 'iscancel', 'warehousename', 'brand', 'countryname', 'isrealization', 'issupply')
    search_fields = ('supplierarticle', 'nmid', 'barcode', 'gnumber', 'srid')
    ordering = ('-date',)

class FinDataAdmin(admin.ModelAdmin):
    list_display = ('rr_dt', 'nmid', 'ts_name')
    ordering = ('rrd_id',)

class SaveDataAdmin(admin.ModelAdmin):
    list_display = ('date_wb', 'nmid', 'size', 'calcType')
    ordering = ('date_wb',)

class RegionSalesAdmin(admin.ModelAdmin):
    list_display = ('date_wb', 'nmid')
    ordering = ('date_wb',)


admin.site.register(RegionSales, RegionSalesAdmin)
admin.site.register(SaveData, SaveDataAdmin)
admin.site.register(FinData, FinDataAdmin)
admin.site.register(Orders, OrdersAdmin)
admin.site.register(Stocks, StocksAdmin)
admin.site.register(nmids, NmidsAdmin)
admin.site.register(CeleryLog, CeleryLogAdmin)
admin.site.register(WbLk)
admin.site.register(Price, PriceAdmin)
admin.site.register(ProductsStat, ProductsStatAdmin)
admin.site.register(Supplies, SuppliesAdmin)
admin.site.register(Betweenwarhouses, BetweenwarhousesAdmin)
admin.site.register(AreaWarehouses, AreaWarehousesAdmin)
admin.site.register(AdvStat, AdvStatAdmin)
admin.site.register(Adverts, AdvertsAdmin)