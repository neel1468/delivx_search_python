from django.conf.urls import url
from Search import views


app_name = 'Search'


urlpatterns = [
    url(r'^filterParameters/$', views.FilterParameters.as_view(), name='FilterParameters'),
    url(r'^storeFilterParameters/$', views.storeFilterParameters.as_view(), name='storeFilterParameters'),
    url(r'^searchFilter/$', views.SearchFilter.as_view(), name='SearchFilter'),
    url(r'^filterProducts/$', views.FilterProducts.as_view(), name='FilterProducts'),
    url(r'^zoneWiseSearchFilter/$', views.ZoneWiseSearchFilter.as_view(), name='ZoneWiseSearchFilter'),
    url(r'^storeSearchFilter/$', views.storeSearchFilter.as_view(), name='storeSearchFilter'),
    url(r'^popularSearchFilter/$', views.PopularSearchFilter.as_view(), name='PopularSearchFilter'),
    url(r'^suggestions/$', views.ProductSuggestions.as_view(), name='ProductSuggestions'),
    url(r'^storeWiseProductSuggestions/$', views.StoreWiseProductSuggestions.as_view(), name='StoreWiseProductSuggestions'),
    url(r'^offerslist/(?P<zoneId>[\w\-]+)/(?P<storeId>[\w\-]+)$', views.OffersList.as_view(), name='OffersList'),
    url(r'^languageList/$', views.LanguageList.as_view(), name='LanguageList'),
    url(r'^cities/$', views.cities.as_view(), name='cities'),
    url(r'^offers/$', views.OffersDetails.as_view(), name='OffersDetails'),

]