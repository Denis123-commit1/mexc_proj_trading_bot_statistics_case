# monitoring_app/api/urls.py
from django.urls import path
from .views import PriceHistoryList, EventList, TradingPairStateList
from .views import TradingPairStateList

urlpatterns = [
    path('price-history/', PriceHistoryList.as_view(), name='price-history'),
    path('events/', EventList.as_view(), name='events'),
    path('trading-pair-states/', TradingPairStateList.as_view(), name='states'),
]