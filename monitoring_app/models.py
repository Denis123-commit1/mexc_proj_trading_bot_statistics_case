# monitoring_project/monitoring_app/models.py

from django.db import models
from datetime import datetime

class TradingPair(models.Model):
    STATUS_CHOICES = (
        ('new', 'New'),
        ('active', 'Active'),
        ('closed', 'Closed'),
    )
    symbol = models.CharField(max_length=50, unique=True)
    exchange = models.CharField(max_length=50, default="MEXC")  # Например, "MEXC", "MEOX" и т.д.
    date_added = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="active")

    def __str__(self):
        return f"{self.symbol} ({self.exchange}) - {self.status}"


class PriceHistory(models.Model):
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE, related_name="price_histories")
    timestamp = models.FloatField()  # Хранится как float (time.time())
    price = models.FloatField()

    def __str__(self):
        return f"{self.trading_pair.symbol} | {self.get_datetime()} | {self.price}"

    def get_datetime(self):
        return datetime.fromtimestamp(self.timestamp)


class Event(models.Model):
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE, related_name="events")
    event_type = models.CharField(max_length=50)  # pump_start, pump_end, new_peak, baseline_set и т.д.
    event_time = models.FloatField()  # время в формате time.time()
    event_price = models.FloatField()
    description = models.TextField(blank=True)

    def __str__(self):
        return f"{self.trading_pair.symbol} | {self.event_type} | {self.get_datetime()} | {self.event_price}"

    def get_datetime(self):
        return datetime.fromtimestamp(self.event_time)
    
    # Пример метода для вычисления продолжительности пампа,
    # если вызвать его для события pump_start и найти ближайшее pump_end
    def pump_duration(self):
        if self.event_type != "pump_start":
            return None
        # Ищем событие pump_end для той же торговой пары, которое произошло после этого события
        pump_end = self.trading_pair.events.filter(event_type="pump_end", event_time__gt=self.event_time).order_by('event_time').first()
        if pump_end:
            duration = pump_end.event_time - self.event_time
            return duration
        return None


class TradingPairState(models.Model):
    """
    Модель для хранения текущего состояния торговой пары (оперативных данных).
    Эти данные можно обновлять при каждом изменении состояния,
    чтобы в случае перезапуска процесса их можно было восстановить и анализировать.
    """
    trading_pair = models.OneToOneField(TradingPair, on_delete=models.CASCADE, related_name="state")
    pump_active = models.BooleanField(default=False)
    peak = models.FloatField(default=0)
    last_ma = models.FloatField(default=0)
    last_updated = models.FloatField(default=0)  # время обновления (time.time())
    last_pump_start = models.FloatField(default=0)  # время начала последнего пампа
    last_pump_duration = models.FloatField(default=0)  # длительность последнего пампа (сек)

    # Новое поле — стартовая цена пампа
    pump_start_price = models.FloatField(default=0)

    def __str__(self):
        return (f"State for {self.trading_pair.symbol}: "
                f"pump_active={self.pump_active}, "
                f"peak={self.peak}, last_ma={self.last_ma}, "
                f"last_pump_start={self.last_pump_start}, "
                f"last_pump_duration={self.last_pump_duration}, "
                f"pump_start_price={self.pump_start_price}")

    def get_last_update_datetime(self):
        return datetime.fromtimestamp(self.last_updated)
    
    def get_last_pump_start_datetime(self):
        if self.last_pump_start:
            return datetime.fromtimestamp(self.last_pump_start)
        return None