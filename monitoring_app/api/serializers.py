# monitoring_app/api/serializers.py
from rest_framework import serializers
from datetime import datetime
from monitoring_app.models import TradingPair, PriceHistory, Event, TradingPairState

class TradingPairStateSerializer(serializers.ModelSerializer):
    last_update = serializers.SerializerMethodField()

    class Meta:
        model = TradingPairState
        fields = ['trading_pair', 'pump_active', 'peak', 'last_ma', 'last_updated', 'last_update']

    def get_last_update(self, obj):
        return obj.get_last_update_datetime().strftime("%Y-%m-%d %H:%M:%S")

class TradingPairSerializer(serializers.ModelSerializer):
    state = TradingPairStateSerializer(read_only=True)

    class Meta:
        model = TradingPair
        fields = ['symbol', 'exchange', 'date_added', 'status', 'state']

class PriceHistorySerializer(serializers.ModelSerializer):
    trading_pair = TradingPairSerializer(read_only=True)
    datetime = serializers.SerializerMethodField()

    class Meta:
        model = PriceHistory
        fields = ['id', 'trading_pair', 'timestamp', 'datetime', 'price']

    def get_datetime(self, obj):
        return obj.get_datetime()

class EventSerializer(serializers.ModelSerializer):
    trading_pair = TradingPairSerializer(read_only=True)
    datetime = serializers.SerializerMethodField()

    class Meta:
        model = Event
        fields = ['id', 'trading_pair', 'event_type', 'event_time', 'datetime', 'event_price', 'description']

    def get_datetime(self, obj):
        return obj.get_datetime()