# monitoring_project/monitoring_app/api/views.py
from rest_framework import generics
from django.utils import timezone
from django_filters.rest_framework import DjangoFilterBackend
from monitoring_app.models import PriceHistory, Event
from .serializers import PriceHistorySerializer, EventSerializer
from rest_framework import generics
from monitoring_app.models import TradingPairState
from .serializers import TradingPairStateSerializer

class PriceHistoryList(generics.ListAPIView):
    queryset = PriceHistory.objects.all().order_by('-timestamp')
    serializer_class = PriceHistorySerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['trading_pair__symbol', 'trading_pair__exchange']

    def get_queryset(self):
        qs = super().get_queryset()
        start_time = self.request.query_params.get('start_time')
        end_time = self.request.query_params.get('end_time')
        if not start_time and not end_time:
            now = timezone.now().timestamp()
            thirty_days_ago = now - 30 * 86400
            qs = qs.filter(timestamp__gte=thirty_days_ago)
        elif start_time and end_time:
            qs = qs.filter(timestamp__gte=start_time, timestamp__lte=end_time)
        return qs

class EventList(generics.ListAPIView):
    queryset = Event.objects.all().order_by('-event_time')
    serializer_class = EventSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['trading_pair__symbol', 'trading_pair__exchange', 'event_type']

    def get_queryset(self):
        qs = super().get_queryset()
        start_time = self.request.query_params.get('start_time')
        end_time = self.request.query_params.get('end_time')
        if not start_time and not end_time:
            now = timezone.now().timestamp()
            thirty_days_ago = now - 30 * 86400
            qs = qs.filter(event_time__gte=thirty_days_ago)
        elif start_time and end_time:
            qs = qs.filter(event_time__gte=start_time, event_time__lte=end_time)
        return qs

class TradingPairStateList(generics.ListAPIView):
    queryset = TradingPairState.objects.all()
    serializer_class = TradingPairStateSerializer