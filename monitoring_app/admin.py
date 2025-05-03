# monitoring_app/admin.py

from django.contrib import admin
from .models import TradingPair, PriceHistory, Event

@admin.register(TradingPair)
class TradingPairAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'exchange', 'date_added', 'status')
    list_filter = ('exchange', 'status')

@admin.register(PriceHistory)
class PriceHistoryAdmin(admin.ModelAdmin):
    list_display = ('trading_pair_symbol', 'timestamp', 'price')
    list_filter = ('trading_pair__symbol',)

    def trading_pair_symbol(self, obj):
        return obj.trading_pair.symbol
    trading_pair_symbol.short_description = 'Symbol'

@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ('trading_pair_symbol', 'event_type', 'event_time', 'event_price', 'pump_duration_display')
    list_filter = ('trading_pair__symbol', 'event_type')

    def trading_pair_symbol(self, obj):
        return obj.trading_pair.symbol
    trading_pair_symbol.short_description = 'Symbol'

    def pump_duration_display(self, obj):
        duration = obj.pump_duration()
        if duration is not None:
            # Переводим в минуты/секунды для удобства
            minutes = int(duration // 60)
            seconds = int(duration % 60)
            return f"{minutes} мин {seconds} сек"
        return "-"
    pump_duration_display.short_description = "Продолжительность пампа"