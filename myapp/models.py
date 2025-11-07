from django.db import models


# Модаль для хранения данных о скорости доставки со складов в ообласти
class AreaWarehouses(models.Model):
    area = models.CharField(max_length=255, unique=True)
    warehouses = models.JSONField()

    class Meta:
        verbose_name_plural = "Области и склады"



# Модель для таблицы wb_lk
class WbLk(models.Model):
    # myapp_wblk
    name = models.CharField(max_length=255)
    token = models.CharField(max_length=400)
    number = models.BigIntegerField(default=0, null=True)
    cookie = models.TextField(default='', null=True)
    authorizev3 = models.TextField(default='', null=True)
    inn = models.BigIntegerField(default=0, null=True)
    tg_id = models.BigIntegerField(default=0, null=True)

    class Meta:
        verbose_name = "Личный кабинет"
        verbose_name_plural = "Личные кабинеты"
    def __str__(self):
        return self.name


class Price(models.Model):
    # myapp_price
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1) #lk_id в бд
    nmid = models.IntegerField(default=0)
    vendorcode = models.CharField(max_length=255, default=0)
    sizes = models.JSONField()  # Массив JSON для хранения размеров
    discount = models.IntegerField()
    clubdiscount = models.IntegerField(default=0)
    editablesizeprice = models.IntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True, null=True)
    blackprice = models.IntegerField(default=0, null=True)
    redprice = models.IntegerField(default=0, null=True)
    spp = models.IntegerField(default=0, null=True)
    wallet_discount = models.IntegerField(default=0, null=True)
    cost_price = models.FloatField(default=0.0, null=True)
    reject = models.IntegerField(default=3, null=True) # брак
    commission = models.IntegerField(default=28, null=True)
    acquiring = models.IntegerField(default=1, null=True)
    drr = models.IntegerField(default=5, null=True)
    usn = models.IntegerField(default=1, null=True)
    nds = models.IntegerField(default=7, null=True)
    main_status = models.BooleanField(default=False)

    class Meta:
        unique_together = ['nmid', 'lk']  # Уникальное ограничение на комбинацию nmID и lk
        verbose_name_plural = "Цены и товары"

    def __str__(self):
        return f"{self.vendorcode} - {self.nmid}"


class ProductsStat(models.Model):
    nmid = models.IntegerField()
    date_wb = models.DateTimeField(auto_now_add=True, null=True) #дата от WB
    openCardCount = models.IntegerField() # Переходы в карточку товара
    addToCartCount = models.IntegerField() # Положили в корзину, шт.
    ordersCount = models.IntegerField() # Заказали товаров, шт.
    ordersSumRub = models.IntegerField() # Заказали на сумму, ₽
    buyoutsCount = models.IntegerField() # Выкупили товаров, шт.
    buyoutsSumRub = models.IntegerField() # Выкупили на сумму, ₽
    cancelCount = models.IntegerField() # Отменили товаров, шт.
    cancelSumRub = models.IntegerField() # Отменили на сумму, ₽
    addToCartConversion = models.IntegerField() # Конверсия в корзину, %
    cartToOrderConversion = models.IntegerField() # Конверсия в заказ, %
    buyoutPercent = models.IntegerField() # Процент выкупа, %

    class Meta:
        unique_together = ['nmid', 'date_wb']  # Уникальное ограничение на комбинацию nmid и date_wb
        verbose_name_plural = "Статистика товаров"


class nmids(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1) #lk_id в бд
    nmid = models.IntegerField() # Артикул WB
    imtid = models.IntegerField() # ID карточки товара. Артикулы WB из одной карточки товара будут иметь одинаковый imtID
    nmuuid = models.CharField(max_length=255) # Внутренний технический ID товара
    subjectid = models.IntegerField() # ID предмета
    subjectname = models.CharField(max_length=255) # Название предмета
    vendorcode = models.CharField(max_length=255) # Артикул продавца
    brand = models.CharField(max_length=255) # Бренд
    title = models.CharField(max_length=500) # Наименование товара
    description = models.TextField(null=True, blank=True) # Описание товара
    needkiz = models.BooleanField() # Требуется ли код маркировки для этого товара
    dimensions = models.JSONField() # Габариты и вес товара c упаковкой, см и кг
    characteristics = models.JSONField() # Характеристики
    sizes = models.JSONField() # Размеры товара
    tag_ids = models.JSONField(default=list)
    created_at = models.DateTimeField() # Дата создания карточки товара (по данным WB)
    updated_at = models.DateTimeField() # Дата изменения карточки товара (по данным WB)
    added_db = models.DateTimeField(auto_now_add=True) # по МСК
    is_active = models.BooleanField(default=True) # поле для понимания нужен им товар или нет

    class Meta:
        unique_together = ['nmid', 'lk']
        verbose_name = "Товар WB"
        verbose_name_plural = "Товары WB"

    def __str__(self):
        return f"{self.nmid} – {self.title} ({self.brand})"


class Supplies(models.Model):
    nmid = models.IntegerField()
    incomeId = models.IntegerField()
    number = models.CharField(max_length=50)
    date_post = models.DateTimeField() # дата поступления (не меняется)
    lastChangeDate = models.DateTimeField() # дата последнего изменения. соответствует параметру dateFrom в запросе (меняется)
    supplierArticle = models.CharField(max_length=255) #Артикул продавца
    techSize = models.CharField()
    barcode = models.CharField(max_length=50)
    quantity = models.IntegerField()
    totalPrice = models.IntegerField() # Цена из УПД
    dateClose = models.DateTimeField() # Дата принятия (закрытия) в WB.
    warehouseName = models.CharField(max_length=255, null=True) #Название склада
    status = models.CharField() #Текущий статус поставки

    class Meta:
        unique_together = ['incomeId', 'nmid']
        verbose_name_plural = "Поставки WB"


# для отправленных поставок на WB
class Betweenwarhouses(models.Model):
    nmid = models.IntegerField()
    vendorcode = models.CharField(max_length=255)  # Артикул продавца
    incomeid = models.IntegerField() # номер поставки
    warehousename = models.CharField(max_length=255, null=True)  # Название склада
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)
    quantity = models.IntegerField(null=True)

    class Meta:
        verbose_name_plural = "Склад -> WB"


class Stocks(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)  # lk_id в бд
    lastchangedate = models.DateTimeField() #Дата и время обновления информации в сервисе. Это поле соответствует параметру dateFrom в запросе. Если часовой пояс не указан, то берётся Московское время (UTC+3)
    warehousename = models.CharField(max_length=255, null=True) #Название склада
    supplierarticle = models.CharField(max_length=255) #Артикул продавца
    nmid = models.IntegerField() #Артикул
    barcode = models.BigIntegerField(null=True) #Баркод
    quantity = models.IntegerField() #Количество, доступное для продажи (сколько можно добавить в корзину)
    inwaytoclient = models.IntegerField() #В пути к клиенту
    inwayfromclient = models.IntegerField() #В пути от клиента
    quantityfull = models.IntegerField(default=0) #Полное (непроданное) количество, которое числится за складом (= quantity + в пути)
    category = models.CharField(max_length=255, null=True) #Категория
    techsize = models.CharField(max_length=255, null=True) #Размер
    issupply = models.BooleanField(default=False) #Договор поставки (внутренние технологические данные)
    isrealization = models.BooleanField(default=False) #Договор реализации (внутренние технологические данные)
    sccode = models.CharField(max_length=255, null=True) #Код контракта (внутренние технологические данные)
    added_db = models.DateTimeField(auto_now_add=True, null=True)  # по МСК время обновления в бд
    updated_at = models.DateTimeField(auto_now_add=True, null=True) # по сути то же что и выше но в UTC
    days_in_stock_last_3 = models.IntegerField(null=True, default=0)
    days_in_stock_last_7 = models.IntegerField(null=True, default=0)
    days_in_stock_last_14 = models.IntegerField(null=True, default=0)
    days_in_stock_last_30 = models.IntegerField(null=True, default=0)

    class Meta:
        unique_together = ['nmid', 'lk', 'supplierarticle', 'warehousename']
        verbose_name_plural = "Отстаки товаров на складах"

    def __str__(self):
        return f"{self.supplierarticle} | {self.techsize} | {self.quantity} шт."


class Orders(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1) #lk_id в бд
    date = models.DateTimeField() #Дата и время заказа. Это поле соответствует параметру dateFrom в запросе, если параметр flag=1. Если часовой пояс не указан, то берётся Московское время (UTC+3)
    lastchangedate = models.DateTimeField() #Дата и время обновления информации в сервисе. Это поле соответствует параметру dateFrom в запросе, если параметр flag=0 или не указан. Московское время (UTC+3).
    warehousename = models.CharField(max_length=255) #Склад отгрузки
    warehousetype = models.CharField(max_length=255) #Тип склада хранения товаров
    countryname = models.CharField(max_length=255) #Страна
    oblastokrugname = models.CharField(max_length=255, null=True) #Округ
    regionname = models.CharField(max_length=255, null=True) #Регион
    supplierarticle = models.CharField(max_length=255) #Артикул продавца
    nmid = models.IntegerField() #Артикул WB
    barcode = models.BigIntegerField(null=True) #Баркод
    category = models.CharField(max_length=255) #Категория
    subject = models.CharField(max_length=255) #Предмет
    brand = models.CharField(max_length=255) #
    techsize = models.CharField(max_length=255, null=True) #Размер товара
    incomeid = models.IntegerField() #Номер поставки
    issupply = models.BooleanField() #Договор поставки
    isrealization = models.BooleanField() #Договор реализации
    totalprice = models.IntegerField() #Цена без скидок
    discountpercent = models.IntegerField() #Скидка продавца
    spp = models.IntegerField() #Скидка WB
    finishedprice = models.FloatField() #Цена с учетом всех скидок, кроме суммы по WB Кошельку
    pricewithdisc = models.FloatField() #Цена со скидкой продавца (= totalPrice * (1 - discountPercent/100))
    iscancel = models.BooleanField() #Отмена заказа. true - заказ отменен
    canceldate = models.DateTimeField() #Дата и время отмены заказа. Если заказ не был отменен, то "0001-01-01T00:00:00".Если часовой пояс не указан, то берётся Московское время UTC+3.
    sticker = models.CharField(max_length=255) #ID стикера
    gnumber = models.CharField() #Номер заказа
    srid = models.CharField(max_length=255) #Уникальный ID заказа. Примечание для использующих API Маркетплейс: srid равен rid в ответах методов сборочных заданий.
    updated_at = models.DateTimeField(auto_now_add=True, null=True)  # время обновления в бд в UTC

    class Meta:
        unique_together = ['nmid', 'lk', 'srid']
        verbose_name = "Заказ WB"
        verbose_name_plural = "Заказы WB"

    def __str__(self):
        return f"{self.supplierarticle} | {self.techsize} | {self.brand} | Заказ: {self.gnumber}"


class CeleryLog(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=50)
    source = models.CharField(max_length=255, null=True)
    message = models.TextField()

    class Meta:
        verbose_name_plural = "Логи"

    def __str__(self):
        return f"[{self.timestamp}] {self.level}: {self.message}"


class AdvStat(models.Model):
    advert_id = models.IntegerField()
    date_wb = models.DateTimeField(auto_now_add=True, null=True)  # дата от WB
    app_type = models.IntegerField()
    nmid = models.IntegerField()
    orders = models.IntegerField()
    atbs = models.IntegerField()
    canceled = models.IntegerField()
    clicks = models.IntegerField()
    cpc = models.IntegerField()
    cr = models.IntegerField()
    ctr = models.IntegerField()
    shks = models.IntegerField()
    sum_cost = models.FloatField(default=0.0, null=True)
    sum_price = models.IntegerField()
    views = models.IntegerField()

    class Meta:
        verbose_name_plural = "Рекламная Стата"
        unique_together = ['nmid', 'date_wb', 'app_type', 'advert_id']


class Adverts(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)  # lk_id в бд
    advert_id = models.IntegerField(unique=True)
    type_adv = models.IntegerField(default=0)
    status = models.IntegerField()
    changeTime = models.DateTimeField(auto_now_add=True, null=True)  # дата от WB

    class Meta:
        verbose_name_plural = "Рекламы"


class FinData(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)
    rrd_id = models.CharField(max_length=255, unique=True) # Номер строки
    rr_dt = models.DateTimeField() # Дата операции
    nmid = models.IntegerField() # Артикул ВБ
    order_dt = models.DateTimeField() # Дата заказа
    sale_dt = models.DateTimeField() # Дата продажи
    shk_id = models.CharField(max_length=255, null=True) # Штрихкод
    ts_name = models.CharField(max_length=255, null=True) # Размер
    supplier_oper_name = models.CharField(max_length=255, null=True) # Основание для оплаты
    retail_price = models.FloatField(null=True) # Цена розничная
    retail_amount = models.FloatField(null=True) # Сумма реализации
    ppvz_for_pay = models.FloatField(null=True) # К перечислению продавцу
    delivery_rub = models.FloatField(null=True) # Стоимость доставки
    storage_fee = models.FloatField(null=True) # Хранение
    deduction = models.FloatField(null=True) # Удержание
    acceptance = models.FloatField(null=True) # Платная приемка
    penalty = models.FloatField(null=True, blank=True) # Штрафы

    class Meta:
        verbose_name_plural = "ФИН отчет"


class SaveData(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)
    date_wb = models.DateTimeField(null=True)  # Дата, за которую был расчёт или перерасчёт
    logWarehouseCoef = models.IntegerField() # Коэффициент логистики и хранения
    officeId = models.IntegerField() # ID склада
    warehouse = models.CharField(max_length=255, null=True) # Название склада
    warehouseCoef = models.IntegerField() # Коэффициент склада
    giId = models.IntegerField() # ID поставки
    chrtId = models.IntegerField() # ID размера для этого артикула WB
    size = models.CharField(max_length=255, null=True) # Размер (techSize в карточке товара)
    barcode = models.CharField(max_length=255, null=True) # Баркод
    subject = models.CharField(max_length=255, null=True) # Предмет
    brand = models.CharField(max_length=255, null=True) # Бренд
    vendorcode = models.CharField(max_length=255, null=True) # Артикул продавца
    nmid = models.IntegerField() # Артикул ВБ
    volume = models.IntegerField() # Объём товара
    calcType = models.CharField(max_length=255, null=True) # Способ расчёта
    warehousePrice = models.IntegerField() # Сумма хранения
    barcodesCount = models.IntegerField() # Количество единиц товара (штук), подлежащих тарифицированию за расчётные сутки
    palletPlaceCode = models.IntegerField() # Код паллетоместа
    palletCount = models.IntegerField() # Количество паллет
    originalDate = models.DateTimeField(null=True) # Если был перерасчёт, это дата первоначального расчёта. Если перерасчёта не было, совпадает с date_wb
    loyaltyDiscount = models.IntegerField() # Скидка программы лояльности, ₽
    tariffFixDate = models.DateTimeField(null=True) # Дата фиксации тарифа
    tariffLowerDate = models.DateTimeField(null=True) # Дата понижения тарифа

    class Meta:
        unique_together = ['date_wb', 'nmid', 'calcType', 'size']
        verbose_name_plural = 'ХРАНЕНИЕ платное'


class RegionSales(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)
    date_wb = models.DateTimeField() # дата отчета
    nmid = models.IntegerField()  # Артикул ВБ
    cityName = models.CharField(max_length=255, null=True) # Населенный пункт
    countryName = models.CharField(max_length=255, null=True) # Страна
    foName = models.CharField(max_length=255, null=True) # Федеральный округ
    regionName = models.CharField(max_length=255, null=True) # Регион
    sa = models.CharField(max_length=255, null=True) # Артикул продавца
    saleInvoiceCostPrice = models.FloatField() # К перечислению за товар, ₽
    saleInvoiceCostPricePerc = models.FloatField()  # Доля, %
    saleItemInvoiceQty = models.IntegerField(null=True) # Выкупили, шт.

    class Meta:
        unique_together = ['date_wb', 'nmid', 'sa', 'cityName', 'regionName']
        verbose_name_plural = 'ПРОДАЖИ по регионам'


class MySklad(models.Model):
    key_id = models.CharField(primary_key=True)
    name = models.CharField(max_length=255, null=True)
    articul = models.CharField(max_length=255, null=True)
    date_time = models.DateTimeField()
    price = models.FloatField()
    quantity = models.FloatField()
    shipped = models.FloatField()
    accepted = models.FloatField()
    color = models.CharField(max_length=255, null=True)
    size = models.CharField(max_length=255, null=True)
    size_ru = models.CharField(max_length=255, null=True)

    class Meta:
        verbose_name_plural = 'Мой Склад'