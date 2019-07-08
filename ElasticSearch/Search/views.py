from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse, JsonResponse
import json
from django.shortcuts import render, redirect
from rest_framework.views import APIView

from elasticsearch import Elasticsearch
import jwt
from datetime import timedelta
from pytz import timezone
import datetime
import requests
import time
from bson import json_util, ObjectId
import asyncio
import pandas as pd
import pymongo
from pymongo import MongoClient, CursorType
from bson.objectid import ObjectId
import os
import geopy.distance
import pytz
from geopy.distance import geodesic
from timezonefinder import TimezoneFinder
tf = TimezoneFinder()
from dateutil import tz
import tzlocal
from symspellpy.symspellpy import SymSpell
# from dotenv import load_dotenv, find_dotenv
import os
# load_dotenv(dotenv_path="/usr/etc/delivXDev")
# load_dotenv(dotenv_path="/usr/etc/deliverX")
# load_dotenv(dotenv_path="/home/embed/searchandOffers/Delivx/search-dev/delivx_search_python/.delivXDev")
# for the spell correction

max_edit_distance_dictionary = 5
prefix_length = 7
sym_spell = SymSpell(max_edit_distance_dictionary, prefix_length)

dictionary_path = "/opt/elasticSearch/ElasticSearch/finalStore.txt"
term_index = 0
count_index = 1

# jwt_token

SERVER_OFFER = "https://offers.deliv-x.com/"

SERVER_DISTANCE = "https://maps.googleapis.com/maps/api/distancematrix/json?origins="
SERVER_DISTANCE_KEY = "AIzaSyBOqakmNqRQtQHS5j1R5yvsr1rQirKoP50"
Secret = "k7Iz6vjw/9NZ8xwO7ES4WphmA+eFYkRtHLNmEArq0pOS+gfnzbSEn8pIeQWYpB6nRo+UgreQ0vGKgmNuay7abt38l1yg8fSuzilzjyhK0ALiw9qTxwLoPFK3Fm4me0Blt2MohZBRr+iHrul0jgo2//81Xz0b2Dk4ScH1ujXWe/mDsMPSho0W1AGqNyC4fCuzOaU67zK/Q9xhasfLcSmhKRc/9tg0krpgSFzU3R3bPfXheo5d2htQl4VPV+OrMiJ+zTfdfMZeijrcv1BqbBqh1oZuP7uA4kSpUj70G43rMQjggcIxlaSBlNVbVq9Sef63arKQesRRWWvRCtHJHSWt5g=="

# es = Elasticsearch(['http://elastic:c24j4svKBM8VtL8FSEM8@159.203.182.173:9200/'])
mongo_url = "mongodb://root_DB:cjdk69RvQy5b5VDL@159.203.182.173:5009/DelivX"
mongo_db = "DelivX"
client = MongoClient(mongo_url)
db = client['DelivX']

elasticsearch_url = "http://elastic:c24j4svKBM8VtL8FSEM8@159.203.182.173:9200/"
es = Elasticsearch([str(elasticsearch_url)])

index_products = "productindex"
doc_type_products = "childProducts"

index_store = "storeindex"
doc_type_store = "stores"

index_popularSearch = "popularindex"
doc_type_popularSearch = "popularSearch"

index_trendingProducts = "trendingindex"
doc_type_trendingProducts = "trendingProducts"

index_offers = "offerindex"
doc_type_offers = "offers"

index_storetime = "workinghourindex"
doc_type_storetype = "workinghour"

timezonename = 'Asia/Kolkata'


class LanguageList(APIView):
    def get(self, request):
        language = []
        language.append({
            'languageName': "English",
            'languageCode': "en"
        })
        try:
            get_language = db.lang_hlp.find({"Active": 1})
            if get_language.count() > 0:
                for i in get_language:
                    language.append({
                        'languageName': i['lan_name'],
                        'languageCode': i['langCode']
                    })
                lag = {
                    'data': language,
                    "message": "Got the Message"}

                return JsonResponse(lag, safe=False, status=200)
            else:
                message = {
                    "message": "Got the Message",
                    "data": language
                }
                return JsonResponse(message, safe=False, status=200)
        except:
            message = {
                    "message": "Got the Message",
                    "data": language
                }
            return JsonResponse(message, safe=False, status=200)


# Suggestions API, Gives all the product suggestions for a particular zone
class ProductSuggestions(APIView):
    def post(self, request):
        filter_responseJson = []
        language = request.META['HTTP_LANGUAGE']
        zoneId = request.META['HTTP_ZONEID'] if 'HTTP_ZONEID' in request.META else "5cbf08b8087d926a151d9954"
        start = time.time()
        try:
            query = request.data
            res = es.search(index=index_products, doc_type=doc_type_products, body=query,
                            filter_path=['hits.hits._id', 'hits.hits._source.productname', 'hits.hits._source.storeId'])
            productData = []
            if len(res) == 0:
                for z in query['query']['bool']['must']:
                    if 'match' in z:
                        if 'storeId' in z['match']:
                            z['match']['zoneId'] = z['match'].pop('storeId')
                            z['match']['zoneId'] = zoneId
                        else:
                            pass
                print("query1", query)
                res = es.search(index=index_products, doc_type=doc_type_products, body=query,
                                filter_path=['hits.hits._id', 'hits.hits._source.productname',
                                             'hits.hits._source.storeId'])

            for i in res['hits']['hits']:
                productData.append({
                    'storeId': i['_source']['storeId'],
                    "productname": i['_source']['productname'][language]
                })
            df = pd.DataFrame(productData)
            filter_responseJson = df['productname'].values.tolist()
            set_filter_responseJson = list(set(filter_responseJson))

            finalSuggestions = {
                "data": set_filter_responseJson,
                "message": "Product Suggestions Found"
            }
            return JsonResponse(finalSuggestions, safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return JsonResponse(error, status=404)


def getStoresDetails(storeId, language):
    q_stores = {
        "query": {
            "bool": {
                "must": [{"match": {"status": 1}}, {"match": {"_id": storeId}}]
            }

        }
    }
    res_stores = es.search(index=index_store, doc_type=doc_type_store, body=q_stores,
                           filter_path=['hits.hits._id', 'hits.hits._source.profileLogos', 'hits.hits._source.storeAddr',
                                        'hits.hits._source.sName', 'hits.hits._source.storeSubCategory',
                                        'hits.hits._source.averageRating', 'hits.hits._source.costForTwo',
                                        'hits.hits._source.coordinates', 'hits.hits._source.avgDeliveryTime',
                                        'hits.hits._source.storedescription'])

    storeId = 0
    storeLogo = 0
    storeAddress = 0
    storeName = 0
    storeSubCategory = []
    averageRating = 0
    if len(res_stores) > 0:
        for storedata in res_stores['hits']['hits']:
            storeId = storedata['_id']
            storeLogo = storedata['_source']['profileLogos']['logoImage']
            storeAddress = storedata['_source']['storeAddr']
            storeName = storedata['_source']['sName'][language]
            storeSubCategory = storedata['_source']['storeSubCategory']
            latitude = storedata['_source']['coordinates']['latitude']
            longitude = storedata['_source']['coordinates']['longitude']
            storedescription = storedata['_source']['storedescription'][language]
            avgdeliverytime = storedata['_source']['avgDeliveryTime']
            averageRating = storedata['_source']['averageRating'] if 'averageRating' in storedata['_source'] else 0
            costForTwo = storedata['_source']['costForTwo'] if 'costForTwo' in storedata['_source'] else 0

        return storeId, storeLogo, storeAddress, storeName, storeSubCategory, averageRating, latitude , longitude, storedescription , avgdeliverytime, costForTwo
    else:
        storeId = 0
        storeLogo = 0
        storeAddress = 0
        storeName = 0
        storeSubCategory = []
        averageRating = 0
        latitude = 0
        longitude = 0
        storedescription = ""
        avgdeliverytime = 0
        costForTwo = 0
        return storeId, storeLogo, storeAddress, storeName, storeSubCategory, averageRating, latitude , longitude, storedescription , avgdeliverytime, costForTwo


def storeQuery(storedf):
    checkStore_query = {
        "query": {
            "bool": {
                "must": [{"match": {"status": 1}}, {"match": {"_id": storedf['storeId']}}]
            }

        }
    }
    return checkStore_query


def storesData(storedf, lan):
    res_stores = es.search(index=index_store, doc_type=doc_type_store, body=storedf['storeQuery'],
                           filter_path=['hits.hits._id', 'hits.hits._source.profileLogos',
                                        'hits.hits._source.storeAddr', 'hits.hits._source.sName',
                                        'hits.hits._source.storeSubCategory', 'hits.hits._source.averageRating'])
    storeId = 0
    storeLogo = 0
    storeAddress = 0
    storeName = 0
    storeSubCategory = []
    averageRating = 0
    if len(res_stores) > 0:
        for storedata in res_stores['hits']['hits']:
            storeId = storedata['_id']
            storeLogo = storedata['_source']['profileLogos']['logoImage']
            storeAddress = storedata['_source']['storeAddr']
            storeName = storedata['_source']['sName'][lan]
            storeSubCategory = storedata['_source']['storeSubCategory']
            averageRating = storedata['_source']['averageRating'] if 'averageRating' in storedata['_source'] else 0
        storeDetails = {
            "storeId": storeId,
            "storeLogo": storeLogo,
            "address": storeAddress,
            "storeName": storeName,
            "storeSubCategory": storeSubCategory,
            "averageRating": averageRating
        }
        return storeDetails
    else:
        storeId = 0
        storeLogo = 0
        storeAddress = 0
        storeName = 0
        storeSubCategory = []
        averageRating = 0
        storeDetails = {
            "storeId": storeId,
            "storeLogo": storeLogo,
            "address": storeAddress,
            "storeName": storeName,
            "storeSubCategory": storeSubCategory,
            "averageRating": averageRating
        }
        return storeDetails


def unitsData(row, lan, curntime, sort):
    unitdata = []
    if len(row['offer']) > 0:
        for j in row['offer']:
            pricedata = []
            if j['status'] == 1 and float(j['endDateTime']) > curntime or j['status'] == 'active' and float(j['endDateTime']) > curntime:
                if j['discountType'] == 0:
                    discount_value = j['discountValue']
                    for k in row['units']:
                        try:
                            if 'availableQuantity' in k:
                                print(k)
                                if k['availableQuantity'] > 0:
                                    availableQuantity = k['availableQuantity']
                                    outOfStock = False
                                else:
                                    availableQuantity = k['availableQuantity']
                                    outOfStock = True
                            else:
                                availableQuantity = 0
                                outOfStock = True
                        except:
                            availableQuantity = 100
                            outOfStock = False
                        pricedata.append({
                            "outOfStock": outOfStock,
                            "availableQuantity": availableQuantity,
                            "unitName": k['name'][lan],
                            "unitId": k['unitId'],
                            "availableQuantity": k['availableQuantity'],
                            "unitPrice": k['price'][lan],
                            "finalPrice": float(k['price']['en']) - float(discount_value),
                            "floatunitPrice": float(k['price']['en']) - float(discount_value),
                            "discount_value": discount_value
                        })

                elif j['discountType'] == 1:
                    for k in row['units']:
                        discount_value = (float(k['price']['en']) * float(j['discountValue'])) / 100
                        try:
                            if 'availableQuantity' in k:
                                if k['availableQuantity'] > 0:
                                    availableQuantity = k['availableQuantity']
                                    outOfStock = False
                                else:
                                    availableQuantity = k['availableQuantity']
                                    outOfStock = True
                            else:
                                availableQuantity = 0
                                outOfStock = True
                        except:
                            availableQuantity = 100
                            outOfStock = False
                        pricedata.append({
                            "outOfStock": outOfStock,
                            "availableQuantity": availableQuantity,
                            "unitName": k['name'][lan],
                            "unitId": k['unitId'],
                            "unitPrice": k['price']['en'],
                            "finalPrice": float(k['price']['en']) - discount_value,
                            "floatunitPrice": float(k['price']['en']) - discount_value,
                            "discount_value": discount_value
                        })
            else:
                for j in row['units']:
                    if 'floatValue' in j:
                        floatprice = j['floatValue']
                    else:
                        floatprice = "none"
                    try:
                        if 'availableQuantity' in j:
                            if j['availableQuantity'] > 0:
                                availableQuantity = j['availableQuantity']
                                outOfStock = False
                            else:
                                availableQuantity = j['availableQuantity']
                                outOfStock = True
                        else:
                            availableQuantity = 0
                            outOfStock = True
                    except:
                        availableQuantity = 100
                        outOfStock = False

                    pricedata.append({
                        "availableQuantity": availableQuantity,
                        "outOfStock": outOfStock,
                        "unitName": j['name'][lan],
                        "unitId": j['unitId'],
                        "unitPrice": j['price'][lan],
                        "finalPrice": floatprice,
                        "floatunitPrice": floatprice,
                        "discount_value": 0
                    })

            # sort the data according to filter apply
            if int(sort) == 0:
                decorated = [(dict_["finalPrice"], dict_) for dict_ in pricedata]
                decorated.sort()
                result = [dict_ for (key, dict_) in decorated]
            elif int(sort) == 1:
                decorated = [(dict_["finalPrice"], dict_) for dict_ in pricedata]
                decorated.sort(reverse=True)
                result = [dict_ for (key, dict_) in decorated]
            else:
                result = pricedata
            return result
    else:
        for j in row['units']:
            if 'floatValue' in j:
                floatprice = j['floatValue']
            else:
                floatprice = "none"
            try:
                if 'availableQuantity' in j:
                    if j['availableQuantity'] > 0:
                        availableQuantity = j['availableQuantity']
                        outOfStock = False
                    else:
                        availableQuantity = j['availableQuantity']
                        outOfStock = True
                else:
                    availableQuantity = 0
                    outOfStock = True
            except:
                availableQuantity = 100
                outOfStock = False
            print(j)
            unitdata.append({
                "availableQuantity": availableQuantity,
                "outOfStock": outOfStock,
                "unitName": j['name'][lan],
                "unitId":    j['unitId'] if 'unitId' in j else "",
                "unitPrice": j['price'][lan],
                "finalPrice": floatprice,
                "floatunitPrice": floatprice,
                "discount_value": 0
            })
        # sort the data according to filter apply
        if int(sort) == 0:
            decorated = [(dict_["finalPrice"], dict_) for dict_ in unitdata]
            decorated.sort()
            result = [dict_ for (key, dict_) in decorated]
        elif int(sort) == 1:
            decorated = [(dict_["finalPrice"], dict_) for dict_ in unitdata]
            decorated.sort(reverse=True)
            result = [dict_ for (key, dict_) in decorated]
        else:
            result = unitdata
        return result


async def search_read(res, start_time, language, filter_responseJson, sname, finalfilter_responseJson_stores,
                      finalfilter_responseJson_products, popularstatus, sort):
    try:
        currdate = datetime.datetime.now()
        addOns = []
        addOnAvailable = "0"
        eastern = timezone(timezonename)
        currlocal = eastern.localize(currdate)
        currlocaltimestamp = currlocal.timestamp()
        currlocalISO = datetime.datetime.fromtimestamp(currlocaltimestamp)
        resData = []
        start = time.time()

        if len(res) <= 0:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return error
        else:
            for i in res['hits']['hits']:
                addOns = []
                addOnsData = []
                currencySymbol = i['_source']['currencySymbol'] if "currencySymbol" in i['_source'] else "₹"
                currency = i['_source']['currency'] if "currency" in i['_source'] else "INR"

                if 'addOns' in i['_source']:
                    if len(['addOns']) > 0:
                        addOnAvailable = 1
                        for j in i['_source']['addOns']:
                            for z in j['addOns']:
                                addOns.append({
                                    "id": z["id"],
                                    "name": z["name"],
                                    "price": z['price'],
                                    "storeAddOnId": z['storeAddOnId']
                                })
                            addOnsData.append({
                                'addOnLimit': j['addOnLimit'] if 'addOnLimit' in j else 0,
                                'addOns': addOns,
                                'id': j['id'],
                                'mandatory': j['mandatory'],
                                'multiple': j['multiple'] if 'multiple' in j else 0,
                                'name': j['name'],
                                'description': j['description'] if 'description' in j else {}
                            })
                    else:
                        addOnAvailable = 0
                        addOnsData = []
                else:
                    addOnAvailable = 0
                    addOnsData = []
                resData.append({
                    "childProductId": i['_id'],
                    "productName": i['_source']['productname'][language],
                    "parentProductId": i['_source']['parentProductId'],
                    "sku": i['_source']['sku'],
                    "CBD": i['_source']['CBD'],
                    "THC": i['_source']['THC'],
                    "currencySymbol": currencySymbol,
                    "currency": currency,
                    "storeId": i['_source']['storeId'],
                    "storeName": i['_source']['store'][language] if 'store' in i['_source'] else "",
                    "mobileImage": i['_source']['images'],
                    "units": i['_source']['units'] if 'units' in i['_source'] else [],
                    "offer": i['_source']['offer'] if 'offer' in i['_source'] else [],
                    "popularstatus": popularstatus,
                    "addOnAvailable": addOnAvailable,
                    "addOns": addOnsData
                })
            dataframe = pd.DataFrame(resData)
            dataframe["popularstatus"] = popularstatus
            dataframe["unitsData"] = dataframe.apply(unitsData, lan=language, curntime=currlocaltimestamp, sort=sort, axis=1)
            dataframe = dataframe.drop_duplicates(subset='childProductId', keep="last")
            details = dataframe.to_json(orient='records')
            data = json.loads(details)

            for k in data:
                try:
                    for q in k['unitsData']:
                        if 'availableQuantity' in q:
                            if q['availableQuantity'] > 0:
                                outOfStock = False
                                availableQuantity = q['availableQuantity']
                            else:
                                outOfStock = True
                                availableQuantity = 0
                        else:
                            outOfStock = True
                            availableQuantity = 0
                except:
                    availableQuantity = 100
                    outOfStock = False

                filter_responseJson.append({
                    "outOfStock": outOfStock,
                    "childProductId": k['childProductId'],
                    "productName": k['productName'],
                    "parentProductId": k['parentProductId'],
                    "sku": k['sku'],
                    "CBD": k['CBD'],
                    "availableQuantity": availableQuantity,
                    "THC": k['THC'],
                    "units": k['unitsData'],
                    "storeId": k['storeId'],
                    "storeName": k['storeName'],
                    "mobileImage": k['mobileImage'],
                    "finalPriceList": k['unitsData'],
                    "offerId": "",
                    "currencySymbol": k['currencySymbol'],
                    "currency": k['currency'],
                    "addOnAvailable": k['addOnAvailable'],
                    "addOns": k['addOns']
                })
            print("parsing stage 1 end", time.time() - start)

            for i in filter_responseJson:
                if i['storeId'] == sname:
                    finalfilter_responseJson_products.append(i)
                    serarchResults_products = {
                        "products": finalfilter_responseJson_products
                    }
                elif sname == str(0):
                    finalfilter_responseJson_products.append(i)
                    serarchResults_products = {
                        "products": finalfilter_responseJson_products
                    }
                else:
                    finalfilter_responseJson_stores.append(i)
                    serarchResults_stores = {
                        "stores": finalfilter_responseJson_stores
                    }


            if len(finalfilter_responseJson_products) != 0:
                finalSearchResults = {
                    "data": serarchResults_products,
                    "message": "Got the details"
                }
            else:
                store_details = []
                storeIDS = []
                for i in serarchResults_stores['stores']:
                    storeIDS.append(i["storeId"])
                for i in list(set(storeIDS)):
                    storeId_store, storeLogo_store, storeAddress_store, storeName_store, storeSubCategory, \
                    averageRating,latitude , longitude, storedescription , \
                    avgdeliverytime, costForTwo = getStoresDetails(i, language)
                    if storeId_store == 0:
                        serarchResults_products = []
                    else:
                        store_details.append(
                            {
                                "businessImage": storeLogo_store,
                                "businessAddress": storeAddress_store,
                                "businessId": storeId_store,
                                "businessName": storeName_store,
                                "averageRating": averageRating,
                                "costForTwo": costForTwo,
                                "latitude": latitude,
                                "longitude": longitude,
                                "estimatedtime": avgdeliverytime,
                                "storeMessage": storedescription
                            }
                        )

                    serarchResults_products = {
                        "stores": store_details
                    }
                    finalSearchResults = {
                        "data": serarchResults_products,
                        "message": "Got the details"
                    }
            print("parsing stage 2 end", time.time() - start)
            print("total time taken", time.time()-start)
            finalSearchResults = {
                "data": serarchResults_products,
                "message": "Got the details"
            }
            return finalSearchResults
    except:
        error = {
            "data": [],
            "message": "Internal Error search_read"
        }
        return JsonResponse(error, status=500)



async def popular_search_write(pname, res_popular_test, start_time, sid, zid, language, storeCategoryId, storeType, res):
    try:
        currdate = datetime.datetime.now()
        eastern = timezone(timezonename)
        currlocal = eastern.localize(currdate)
        currlocaltimestamp = currlocal.timestamp()
        currlocalISO = datetime.datetime.fromtimestamp(currlocaltimestamp)
        temp_count = 0
        if len(res_popular_test) == 0:
            for i in res['hits']['hits']:
                del i['_id']
                i['_source']['count'] = 0
                res_popular = es.index(index=index_popularSearch, doc_type=doc_type_popularSearch, body=i['_source'])
        else:
            for i in res_popular_test['hits']['hits']:
                if i['_source']['productname']['en'] == pname:
                    temp_count += 1
                    iddata = i['_id']
                    final_count = i['_source']['count']

                else:
                    pass

            if temp_count == 0:
                for i in res['hits']['hits']:
                    del i['_id']
                    i['_source']['count'] = 0
                    res_popular = es.index(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                           body=i['_source'])
            else:
                final_count_final = int(final_count) + 1
                res_pop_update = es.update(index=index_popularSearch, doc_type=doc_type_popularSearch, id=iddata,
                                           body={"doc": {"count": final_count_final}})

        print("end", time.time() - start_time)

    except:
        error = {
            "data": [],
            "message": "Internal Error popular_search_write"
        }
        return JsonResponse(error, status=500)


class PopularSearchFilter(APIView):
    '''
        API for the Popular Search Filter
        for the products which product is popular in the list
    '''
    def post(self, request):
        count_list = []
        count_dict = {}
        language = request.META['HTTP_LANGUAGE']
        start = time.time()
        try:
            popular_search_query = request.data
            res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                         body=popular_search_query,
                                         filter_path=['hits.hits._id', 'hits.hits._source.productname.'+language,
                                                      'hits.hits._source.count'])
            for i in res_popular_test['hits']['hits']:
                count_dict[i['_source']['productname'][language]] =  int(i['_source']['count'])
            final_popular_list = sorted(count_dict, key=count_dict.get, reverse=True)[:10]
            final_popular_list = sorted(count_dict, key=count_dict.get, reverse=True)[:10]
            Final_output = {
                "data": final_popular_list,
                "message": "Got the Message"
            }
            print("total time taken", time.time()-start)
            return JsonResponse(Final_output, safe=False)
        except:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return JsonResponse(error, status=404)


# This API populates Filter Parameters based on a key
class FilterParameters(APIView):
    def post(self, request):
        start_time = time.time()
        print(start_time)
        try:
            final_filter_parameters = []
            price_filter_parameters = []
            language = request.META['HTTP_LANGUAGE']
            final_filterType = int(request.META['HTTP_FILTERTYPE'])
            filter_parameters_query = request.data

            filterTypesList = {
                1: "catName",
                2: "subCatName",
                3: "subSubCatName",
                4: "brandTitle",
                5: "manufactureName",
                6: "price",
                7: "colors",
            }
            if request.META['HTTP_POPULARSTATUS'] == "0":
                print("no trending")
                print("Es query start", time.time() - start_time, "to run")

                # Es Query to get all the filter parameters
                res_filter_parameters = es.search(index=index_products, doc_type=doc_type_products,
                                                  body=filter_parameters_query,
                                                  # filter_path=['hits.hits._id', 'hits.hits._source'])
                                                  filter_path=['hits.hits._id', 'hits.hits._source.units',
                                                               'hits.hits._source.storeId', 'hits.hits._source.zoneId',
                                                               'hits.hits._source.brandTitle',
                                                               'hits.hits._source.manufactureName',
                                                               'hits.hits._source.colors',
                                                               'hits.hits._source.catName',
                                                               'hits.hits._source.subCatName',
                                                               'hits.hits._source.subSubCatName',
                                                               'hits.hits._source.offer',
                                                               'hits.hits._source.currencySymbol',
                                                               'hits.hits._source.currency'
                                                               ])
                print("Es query end", time.time() - start_time, "to run")
            else:
                print("inside trending")
                print("Es query start", time.time() - start_time, "to run")

                # Es Query to get all the filter parameters

                res_filter_parameters = es.search(index=index_trendingProducts, doc_type=doc_type_trendingProducts,
                                                  body=filter_parameters_query,
                                                  # filter_path=['hits.hits._id', 'hits.hits._source'])
                                                  filter_path=['hits.hits._id', 'hits.hits._source.units',
                                                               'hits.hits._source.storeId',
                                                               'hits.hits._source.brandTitle',
                                                               'hits.hits._source.manufactureName',
                                                               'hits.hits._source.colors',
                                                               'hits.hits._source.catName',
                                                               'hits.hits._source.subCatName',
                                                               'hits.hits._source.subSubCatName',
                                                               'hits.hits._source.offer',
                                                               'hits.hits._source.currencySymbol',
                                                               'hits.hits._source.currency'
                                                               ])
                print("Es query end", time.time() - start_time, "to run")

            # gets all the required data for a particular filter type
            for i in res_filter_parameters['hits']['hits']:
                if "currencySymbol" in i["_source"]:
                    currencySymbol = i["_source"]['currencySymbol']
                else:
                    currencySymbol = "₹"
                    print("no currency")
                if final_filterType == 1:
                    if len(i["_source"][filterTypesList[final_filterType]][language]) > 0:
                        final_filter_parameters.append(i["_source"][filterTypesList[final_filterType]][language])
                        storeId = i["_source"]['storeId']

                elif final_filterType == 2:
                    if len(i["_source"]["subCatName"][language]) > 0:
                        final_filter_parameters.append(i["_source"]["subCatName"][language])

                elif final_filterType == 3:
                    if len(i["_source"]["subSubCatName"][language]) > 0:
                        final_filter_parameters.append(i["_source"]["subSubCatName"][language])

                elif final_filterType == 4:
                    if len(i["_source"]['brandTitle'][language]) > 0:
                        final_filter_parameters.append(i["_source"]['brandTitle'][language])
                    # else:
                    #     print("empty brandTitle")

                elif final_filterType == 5:
                    if len(i["_source"]['manufactureName'][language]) > 0:
                        final_filter_parameters.append(i["_source"]['manufactureName'][language])
                    # else:
                    #     print("empty manufactureNanme")

                elif final_filterType == 6:
                    for unit in i["_source"]['units']:
                        if 'floatValue' in unit:
                            price_filter_parameters.append(float(unit['floatValue']))
                        else:
                            print("No Float value")
                            print(i["_id"])
                            price_filter_parameters.append(float(0))

                elif final_filterType == 7:
                    for color in i["_source"][filterTypesList[final_filterType]]:
                        if 'colorName' in color:
                            final_filter_parameters.append(color['colorName'])

            print("Es Parsing end", time.time() - start_time, "to run")

            if final_filterType == 6:
                print(price_filter_parameters)
                final_filter_parameters.append(float(max(price_filter_parameters)))

            Final_output = {
                "data": list(set(final_filter_parameters)),
                "currency": currencySymbol,
                "message": "Got the Message"
            }
            print("==============", Final_output)
            print("program took", time.time() - start_time, "to run")

            return JsonResponse(Final_output, safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return JsonResponse(error, status=404)


class SearchFilter(APIView):
    '''
            for popular status
                0 for normal item search
                1 for trending
                2 for popular search
        '''
    def post(self, request):
        try:
            start_time = time.time()
            finalfilter_responseJson_products = []
            finalfilter_responseJson_stores = []
            filter_responseJson = []
            offerJson = []
            language = request.META['HTTP_LANGUAGE']
            storeId = request.META['HTTP_STOREID']
            zoneId = request.META['HTTP_ZONEID']
            print("============================================popular status=====", request.META['HTTP_POPULARSTATUS'])
            print("============================================", storeId)
            if storeId == "0":
                checkoffers = requests.get(SERVER_OFFER + 'offerslist/' + str(zoneId) + "/" + str(0),
                                           headers={"authorization": request.META['HTTP_AUTHORIZATION'],
                                                    "language": language})
            else:
                checkoffers = requests.get(SERVER_OFFER + 'offerslist/' + str(zoneId) + "/" + str(storeId),
                                           headers={"authorization": request.META['HTTP_AUTHORIZATION'],
                                                    "language": language})

            if 'HTTP_SEARCHEDITEM' in request.META:
                searchedItem = request.META['HTTP_SEARCHEDITEM']
            print("query", request.data)
            search_item_query = request.data
            if request.META['HTTP_POPULARSTATUS'] == '0':
                if type(search_item_query) == str or type(search_item_query) == '':
                    search_item_query = json.loads(search_item_query)

                if storeId != '0':
                    for z in search_item_query['query']['bool']['must']:
                        if 'match' in z:
                            if 'zoneId' in z['match']:
                                z['match']['storeId'] = z['match'].pop('zoneId')
                                z['match']['storeId'] = storeId
                            else:
                                pass

                res = es.search(index=index_products, doc_type=doc_type_products, body=search_item_query,
                                filter_path=['hits.hits._id', 'hits.hits._source.CBD', 'hits.hits._source.THC',
                                             'hits.hits._source.images', 'hits.hits._source.parentProductId',
                                             'hits.hits._source.productname', 'hits.hits._source.sku',
                                             'hits.hits._source.storeId', 'hits.hits._source.store',
                                             'hits.hits._source.units',
                                             'hits.hits._source.addOns',
                                             'hits.hits._source.currencySymbol', 'hits.hits._source.currency',
                                             'hits.hits._source.offer'])
                if len(res) <= 0:
                    final_json = {
                        "data": [],
                        "message": "No Data Found",
                    }
                    return JsonResponse(final_json, safe=False, status=404)

                # es read end

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if 'sort' in search_item_query:
                    if 'units.floatValue' in search_item_query['sort']:
                        if search_item_query['sort']['units.floatValue']['order'] == "asc":
                            sort = 0
                        elif search_item_query['sort']['units.floatValue']['order'] == "desc":
                            sort = 1
                    else:
                        sort = 2
                else:
                    sort = 3
                print("============sort=================", sort)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                      "query": {
                        "match_phrase_prefix": {"productName": searchedItem}
                      }
                    }
                    res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    res_popular_result = es.search(index=index_products, doc_type=doc_type_products,
                                                 body=search_item_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    # es write end
                    data = loop.run_until_complete(asyncio.gather(
                        search_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort),
                        popular_search_write(searchedItem, res_popular_test, start_time, storeId,
                                             zoneId, language, request.META['HTTP_STORECATEGORYID'],
                                             request.META['HTTP_STORETYPE'], res_popular_result)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        search_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))

                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)

            elif request.META['HTTP_POPULARSTATUS'] == '2':
                if type(search_item_query) == str or type(search_item_query) == '':
                    search_item_query = json.loads(search_item_query)

                if 'sort' in search_item_query:
                    if 'units.floatValue' in search_item_query['sort']:
                        if search_item_query['sort']['units.floatValue']['order'] == "asc":
                            sort = 0
                        elif search_item_query['sort']['units.floatValue']['order'] == "desc":
                            sort = 1
                    else:
                        sort = 2
                else:
                    sort = 3
                if storeId != '0':
                    for z in search_item_query['query']['bool']['must']:
                        if 'match' in z:
                            if 'zoneId' in z['match']:
                                z['match']['storeId'] = z['match'].pop('zoneId')
                                z['match']['storeId'] = storeId
                            else:
                                pass
                res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                             body=search_item_query,
                                             filter_path=['hits.hits._id', 'hits.hits._source'])

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)

                data = loop.run_until_complete(asyncio.gather(
                    search_read(res_popular_test, start_time, language, filter_responseJson,
                                storeId, finalfilter_responseJson_stores,
                                finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))
                loop.close()
                if len(data[0]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)

            else:
                print("----------------------------for trending------------------------------------------------------")
                print(search_item_query)

                if 'sort' in search_item_query:
                    if 'units.floatValue' in search_item_query['sort']:
                        if search_item_query['sort']['units.floatValue']['order'] == "asc":
                            sort = 0
                        elif search_item_query['sort']['units.floatValue']['order'] == "desc":
                            sort = 1
                    else:
                        sort = 2
                else:
                    sort = 3

                res = es.search(index=index_trendingProducts, doc_type=doc_type_trendingProducts, body=search_item_query,
                                filter_path=['hits.hits._id',
                                             'hits.hits._source.childProductId',
                                             'hits.hits._source.productname',
                                             'hits.hits._source.storeId',
                                             'hits.hits._source.units',
                                             'hits.hits._source.offer', 'hits.hits._source.images'])

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                        "query": {
                            "bool": {
                                "must": [{"match": {"productName": searchedItem}}]
                            }
                        }
                    }
                    res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])


                    data = loop.run_until_complete(asyncio.gather(
                        search_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort),
                        popular_search_write(searchedItem, res_popular_test, start_time, storeId,
                                             zoneId, language, res_popular_result, res_popular_test)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        search_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))
                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error,safe=False, status=500)


class OffersList(APIView):
    def get(self, request, zoneId, storeId):
        try:
            jsondata = []
            print(request.META)
            language = request.META['HTTP_LANGUAGE']
            if storeId == '0':
                print("zone data")
                offerquery = db.PythonOffersTest.find({"status": 1, "zones": zoneId})
            else:
                print("store data")
                offerquery = db.PythonOffersTest.find({"status": 1, "zones": zoneId, "storeId": storeId})
            offers = offerquery
            offerscount = offers.count()
            print(offerscount)
            if offerscount != 0:
                for i in offers:
                    citydata = db.cities.find({})
                    for j in citydata:
                        for k in j['cities']:
                            if k['cityId'] == ObjectId(str(i['cityId'])):
                                cityName = k['cityName']
                    print(cityName)
                    storedata = db.stores.find({"_id": ObjectId(i["storeId"])})
                    for j in storedata:
                        storeName = j["sName"][language]
                    print(storeName)
                    if len(i["franchiseID"]) != 0:
                        franchisedata = db.franchise.find({"_id": ObjectId(i["franchiseID"])})
                        for j in franchisedata:
                            franchisedata = j["name"][language]
                    else:
                        franchisedata = ""

                    jsondata.append({
                        "offerId": str(i['_id']),
                        "name": i['name']['en'],
                        "city": cityName,
                        "franchise": franchisedata,
                        "storeName": storeName,
                        "applicableOnStatus": i['applicableOnStatus'],
                        "offerTypeString": i['offerTypeString'],
                        "discountValue": int(i['discountValue']),
                        "minimumPurchaseQty": int(i['minimumPurchaseQty']),
                        "startDateTimeISO": i['startDateTimeISO'],
                        "endDateTimeISO": i['endDateTimeISO'],
                        "globalClaimCount": int(i['globalClaimCount']),
                        "statusString": i['statusString'],
                        "status": int(i['status']),
                        "description": i['description']['en'],
                        "images": i['images'],
                        "franchiseName": franchisedata,
                        "cityName": cityName,
                        "storeId": i['storeId'],
                        "globalUsageLimit": int(i['globalUsageLimit']),
                        "perUserLimit": int(i['perUserLimit']),
                        "products": i["products"],
                        "zones": i['zones'],
                        "startDateTime": int(i['startDateTime']),
                        "endDateTime": int(i['endDateTime']),
                        "offerType": int(i['offerType']),
                        "applicableOnString": i['applicableOnStatus'],
                        "applicableOn": int(i['applicableOn']),
                        "termscond": str(i["termscond"]),
                        "howItWorks": str(i["howItWorks"])
                    })
                final_json = {
                    "data": jsondata,
                    "message": "success",
                    "totalCount": offerscount
                }
                return JsonResponse(final_json, safe=False, status=200)
            else:
                final_json = {
                    "data": jsondata,
                    "message": "no data found",
                    "totalCount": offerscount
                }
                return JsonResponse(final_json, safe=False, status=404)
        except:
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error, safe=False, status=500)


class storeSearchFilter(APIView):
    def post(self, request):
        try:
            # print(request.META)
            storeList = []
            start = time.time()
            storeIsOpen = False
            today = False
            nextOpenTime = ''
            language = request.META['HTTP_LANGUAGE']
            # timezonename = tf.timezone_at(lng=float(request.META['HTTP_LONGITUDE']), lat=float(request.META['HTTP_LATITUDE']))  # returns 'Europe/Berlin'
            timezonename = "Asia/Calcutta"
            currdate = datetime.datetime.now()
            current_hour = datetime.datetime.now(pytz.timezone(timezonename)).hour
            current_minute = datetime.datetime.now(pytz.timezone(timezonename)).minute
            search_item_query = request.data
            print(search_item_query)
            if not sym_spell.load_dictionary(dictionary_path, term_index, count_index):
                print("Dictionary file not found")

            if type(search_item_query) == str or type(search_item_query) == '':
                search_item_query = json.loads(search_item_query)

            for i in search_item_query['query']['bool']['must']:
                if 'match_phrase_prefix' in i:
                    if 'sName.en' in i['match_phrase_prefix']:
                        input_term = i['match_phrase_prefix']['sName.en']
                        if len(input_term) > 3:
                            result = sym_spell.word_segmentation(input_term)
                            i['match_phrase_prefix']['sName.en'] = i['match_phrase_prefix'].pop('sName.en')
                            i['match_phrase_prefix']['sName.en'] = result.corrected_string
                        else:
                            pass
                else:
                    pass
            print("======================================", search_item_query)

            res = es.search(index=index_store, doc_type=doc_type_store, body=search_item_query,
                            filter_path=['hits.hits._id', 'hits.hits._source.sName', 'hits.hits._source.storeAddr',
                                         'hits.hits._source.storeSubCategory', 'hits.hits._source.foodTypeName',
                                         'hits.hits._source.foodType',
                                         'hits.hits._source.streetName',
                                         'hits.hits._source.localityName',
                                         'hits.hits._source.areaName',
                                         'hits.hits._source.cartsAllowed',
                                         'hits.hits._source.costForTwo', 'hits.hits._source.storeDescription',
                                         'hits.hits._source.profileLogos', 'hits.hits._source.bannerLogos',
                                         'hits.hits._source.storeBillingAddr',
                                         'hits.hits._source.franchiseId', 'hits.hits._source.franchiseName',
                                         'hits.hits._source.minimumOrder', 'hits.hits._source.freeDeliveryAbove',
                                         'hits.hits._source.storeType', 'hits.hits._source.storeTypeMsg',
                                         'hits.hits._source.coordinates', 'hits.hits._source.averageRating',
                                         'hits.hits._source.storeCategory', 'hits.hits._source.storeCategory',
                                         'hits.hits._source.storeIsOpen', 'hits.hits._source.nextOpenTime',
                                         'hits.hits._source.nextCloseTime',
                                         'hits.hits._source.storeType'])
            if len(res) <= 0:
                error = {
                    "data": [],
                    "message": "No Products Found"
                }
                return JsonResponse(error, safe=False, status=404)
            else:
                for i in res['hits']['hits']:
                    if 'sName' in i['_source']:
                        coords_lat_1 = i['_source']['coordinates']['latitude']
                        coords_long_1 = i['_source']['coordinates']['longitude']

                        coords_lat_2 = request.META['HTTP_LATITUDE']
                        coords_long_2 = request.META['HTTP_LONGITUDE']

                        if len(i['_source']['storeSubCategory']) > 0:
                            for j in i['_source']['storeSubCategory']:
                                cuisin = j['subCategoryName'][language]
                        else:
                            print("else")
                            cuisin = ""
                        resultMiles = 0
                        resultKm = 0
                        coords_1 = (coords_lat_1, coords_lat_2)
                        coords_2 = (float(request.META['HTTP_LATITUDE']), float(request.META['HTTP_LONGITUDE']))
                        resultMiles = geopy.distance.vincenty(coords_1, coords_2).miles * 0.000621371192
                        resultKm = geopy.distance.vincenty(coords_1, coords_2).km / 1000
                        storeList.append({
                            "storeId": i['_id'],
                            'storeIsOpen': i['_source']['storeIsOpen'],
                            'nextOpenTime':i['_source']['nextOpenTime'],
                            'nextCloseTime':i['_source']['nextCloseTime'],
                            "storeAddr": i['_source']['storeAddr'],
                            "cartsAllowed": i['_source']['cartsAllowed'],
                            "storeBillingAddr": i['_source']['storeBillingAddr'] if 'storeBillingAddr' in i[
                                '_source'] else "",
                            "franchiseId": i['_source']['franchiseId'] if 'franchiseId' in i['_source'] else "",
                            "averageRating": i['_source']['averageRating'] if 'averageRating' in i['_source'] else "",
                            "franchiseName": i['_source']['franchiseName'] if 'franchiseName' in i['_source'] else "",
                            "minimumOrder": i['_source']['minimumOrder'] if 'minimumOrder' in i['_source'] else "",
                            "freeDeliveryAbove": i['_source']['freeDeliveryAbove'] if 'freeDeliveryAbove' in i[
                                '_source'] else "",
                            "streetName": i['_source']['streetName'] if 'streetName' in i['_source'] else "",
                            "localityName": i['_source']['localityName'] if 'localityName' in i['_source'] else "",
                            "areaName": i['_source']['areaName'] if 'areaName' in i['_source'] else "",
                            "storeType": i['_source']['storeType'] if 'storeType' in i['_source'] else "",
                            "storeTypeMsg": i['_source']['storeTypeMsg'] if 'storeTypeMsg' in i['_source'] else "",
                            "storeName": i['_source']['sName'][language] if 'sName' in i['_source'] else "",
                            "storeDescription": i['_source']['description'] if 'description' in i['_source'] else "",
                            "distanceMiles": resultMiles,
                            "distanceKm": resultKm,
                            "distance": resultKm,
                            "storeSubCategory": cuisin,
                            "foodTypeName": i['_source']['foodTypeName'] if 'foodTypeName' in i['_source'] else "",
                            "costForTwo": i['_source']['costForTwo'] if 'costForTwo' in i['_source'] else "",
                            "logoImage": i['_source']['profileLogos']['logoImage'] if 'profileLogos' in i[
                                '_source'] else "",
                            "bannerimage": i['_source']['bannerLogos']['bannerimage'] if 'bannerLogos' in i[
                                '_source'] else ""
                        })

                    else:
                        print(i['_id'])
                        print("missing some important fields")
                print("last time", time.time()-start)
                Final_output = {
                    "data": storeList,
                    "message": "Got the Message"
                }
                print(Final_output)
            return JsonResponse(Final_output, safe=False)
        except:
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error, safe=False, status=500)


class storeFilterParameters(APIView):
    def post(self, request):
        try:
            filterParameters = []
            webFilterParameters = []
            currency = "INR"
            currencySymbol = "₹"
            language = request.META['HTTP_LANGUAGE']
            final_filterType = int(request.META['HTTP_FILTERTYPE'])
            filter_parameters_query = request.data
            print("Query", filter_parameters_query)
            res = es.search(index=index_store, doc_type=doc_type_store, body=filter_parameters_query,
                            filter_path=['hits.hits._id', 'hits.hits._source.foodTypeName',
                                         'hits.hits._source.foodType', 'hits.hits._source.costForTwo',
                                         'hits.hits._source.storeSubCategory','hits.hits._source.subCategoryId', 'hits.hits._source.averageRating',
                                         'hits.hits._source.storeCategory', 'hits.hits._source.storeType',
                                         'hits.hits._source.currency', 'hits.hits._source.currencySymbol'
                                         ])
            if len(res) <= 0:
                error = {
                    "data": [],
                    "message": "No Products Found"
                }
                return JsonResponse(error, safe=False, status=404)
            else:
                for i in res['hits']['hits']:
                    if '_source' in i:
                        if 'currency' in i['_source']:
                            currency = i['_source']['currency']

                        if 'currencySymbol' in i['_source']:
                            currencySymbol = i['_source']['currencySymbol']

                        if int(final_filterType) == 1:
                            if 'foodTypeName' in i['_source']:
                                filterParameters.append(i['_source']['foodTypeName'])
                                final_filterParameters = list(set(filterParameters))
                                webfinal_filterParameters = []
                        elif int(final_filterType) == 2:
                            if 'costForTwo' in i['_source']:
                                filterParameters.append(i['_source']['costForTwo'])
                            else:
                                temp = 0

                        elif int(final_filterType) == 3:
                            if 'storeSubCategory' in i['_source']:
                                for j in i['_source']['storeSubCategory']:
                                    filterParameters.append(j['subCategoryName'][language])
                                    webFilterParameters.append({
                                        "subCatName": j['subCategoryName'][language],
                                        "subCatId": str(j['subCategoryId']),
                                        "subCatIcon": j['subCategoryIconImage'] if 'subCategoryIconImage' in j else ""
                                    })
                                final_filterParameters = list(set(filterParameters))
                                webfinal_filterParameters = {each['subCatId'] : each for each in webFilterParameters }.values()
                                # print(list(webfinal_filterParameters))

                        elif int(final_filterType) == 4:
                            if 'averageRating' in i['_source']:
                                filterParameters.append(i['_source']['averageRating'])
                            else:
                                temp = 0
                        else:
                            print("else")
                            error = {
                                "data": [],
                                "message": "No Products Found",
                                "currencySymbol": currencySymbol,
                                "currency": currency
                            }
                            return JsonResponse(error, safe=False, status=404)
            if int(final_filterType) == 2:
                temp = float(max(filterParameters))
                final_filterParameters = [temp]
                webfinal_filterParameters = []

            if int(final_filterType) == 4:
                if len(filterParameters) > 0:
                    temp = float(max(filterParameters))
                    print(filterParameters)
                    final_filterParameters = [temp]
                    webfinal_filterParameters = []
                else:
                    temp = 5.0
                    print(filterParameters)
                    final_filterParameters = [temp]
                    webfinal_filterParameters = []

            Final_output = {
                "data": final_filterParameters,
                "webData": list(webfinal_filterParameters),
                "currencySymbol": currencySymbol,
                "currency": currency,
                "message": "Got the Message"
            }

            return JsonResponse(Final_output, safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error, safe=False, status=500)


def storeQuery(row):
    '''
    function for generate the query for the store find
    :param row:
    :return:
    '''
    searchStoreTime_item_query = {"from": 0,"size": 10,"query": {"bool": {"must": [{"match": {"status": 1}},{"match": {"storeId": str(row['businessId'])}}]}}}
    return searchStoreTime_item_query


def storeDistance(row, latitude, longtitude):
    '''
    function for the find the store distance
    :param row:
    :param latitude:
    :param longtitude:
    :return:
    '''
    coords_1 = (row['lat'], row['lng'])
    coords_2 = (latitude, longtitude)
    return geopy.distance.vincenty(coords_1, coords_2).miles


def storeDistanceKm(row, latitude, longtitude):
    '''
    function for the find the store distance
    :param row:
    :param latitude:
    :param longtitude:
    :return:
    '''
    coords_1 = (row['lat'], row['lng'])
    coords_2 = (latitude, longtitude)
    return geopy.distance.vincenty(coords_1, coords_2).km


def checkStoreTime(df):
    checkStoretime = es.search(index=index_storetime, doc_type=doc_type_storetype,
                               body= df['storetimeQuery'],
                               filter_path=['hits.hits._id', 'hits.hits._source.startTime',
                                            'hits.hits._source.endTime',
                                            'hits.hits._source.startTimeDate',
                                            'hits.hits._source.endTimeDate'])
    if 'hits' in checkStoretime:
        for times in checkStoretime['hits']['hits']:
            startDateTime = times['_source']['startTime']
            startDate = times['_source']['startTimeDate']
            endDate = times['_source']['endTimeDate']

            start_utc_time = datetime.datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
            start_object = start_utc_time.replace(tzinfo=pytz.utc).astimezone(
                pytz.timezone(timezonename))

            end_utc_time = datetime.datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")
            end_object = end_utc_time.replace(tzinfo=pytz.utc).astimezone(
                pytz.timezone(timezonename))

            start_hour = start_object.hour
            start_minut = start_object.minute
            startTime = str(start_hour) + ':' + str(start_minut) + ':00'

            end_hour = end_object.hour
            end_minut = end_object.minute
            endTime = str(end_hour) + ':' + str(end_minut) + ':00'

            currentTime = str(current_hour) + ':' + str(current_minute) + ':00'

            if currentTime > startTime and currentTime < endTime:
                storeIsOpen = True
                today = True
                nextOpenTime = startDateTime
            elif currentTime < startTime and currentTime > endTime:
                storeIsOpen = False
                today = True
                nextOpenTime = startDateTime
            elif currentTime > startTime and currentTime > endTime:
                storeIsOpen = False
                today = False
                nextOpenTime = startDateTime
            elif currentTime > endTime and currentTime < startTime:
                storeIsOpen = False
                today = False
                nextOpenTime = startDateTime
        return storeIsOpen, today, nextOpenTime
    else:
        return False, False, False


class StoreWiseProductSuggestions(APIView):
    '''
        API Find the Products by the Store Wise
    '''
    def post(self, request):
        print("called")
        start = time.time()
        storeIsOpen = False
        today = False
        nextOpenTime = ''
        timezonename = tf.timezone_at(lng=float(request.META['HTTP_LONGITUDE']),lat=float(request.META['HTTP_LATITUDE']))  # returns 'Europe/Berlin'
        currdate = datetime.datetime.now()
        current_hour = datetime.datetime.now(pytz.timezone(timezonename)).hour
        current_minute = datetime.datetime.now(pytz.timezone(timezonename)).minute
        print(request.data)
        filter_responseJson = []
        storeData = []
        storefilter_responseJson = []
        filterids_responseJson = []
        storefilterids_responseJson = []
        language = request.META['HTTP_LANGUAGE']
        zoneId = request.META['HTTP_ZONEID']
        storeType = 1
        searchedItem = request.META['HTTP_SEARCHEDITEM']
        try:
            query = request.data
            print('query for the product', query)
            res = es.search(index=index_products, doc_type=doc_type_products, body=query,filter_path=['hits.hits._id', 'hits.hits._source.productname', 'hits.hits._source.storeId'])
            if len(res) > 0:
                for i in res['hits']['hits']:
                    filter_responseJson.append(i['_source']['productname'][language])
                    filterids_responseJson.append(i['_source']['storeId'])
                    set_filter_responseJson = list(set(filter_responseJson))
                    set_filterids_responseJson = list(set(filterids_responseJson))
                    finalSuggestions = {
                        "data": set_filter_responseJson,
                        "message": "Product Suggestions Found"
                    }
            else:
                print("No products in that name")
                set_filterids_responseJson = []
                finalSuggestions = {
                    "data": [],
                    "message": "Product Suggestions Found"
                }

            print("-------------------stores--------------------")
            for z in query['query']['bool']['must']:
                if 'match' in z:
                    if 'storeType' in z['match']:
                        storeType = z['match']['storeType']
                    else:
                        pass
            checkStorename_query = {"from": 0, "query": {"bool": {"must": [{"match": {"status": 1}}, {"match": {"serviceZones": zoneId}},
                                                                           {"match": {"storeType": storeType}},{"match_phrase_prefix": {"sName." + language: searchedItem}}]}}, "size": 10}
            storeList = []
            res = es.search(index=index_store, doc_type=doc_type_store, body=checkStorename_query,
                            filter_path=['hits.hits._id', 'hits.hits._source.sName', 'hits.hits._source.storeAddr',
                                         'hits.hits._source.storeSubCategory', 'hits.hits._source.foodTypeName',
                                         'hits.hits._source.foodType',
                                         'hits.hits._source.cartsAllowed',
                                         'hits.hits._source.costForTwo', 'hits.hits._source.storeDescription',
                                         'hits.hits._source.profileLogos', 'hits.hits._source.bannerLogos',
                                         'hits.hits._source.storeBillingAddr',
                                         'hits.hits._source.franchiseId', 'hits.hits._source.franchiseName',
                                         'hits.hits._source.minimumOrder', 'hits.hits._source.freeDeliveryAbove',
                                         'hits.hits._source.storeType', 'hits.hits._source.storeTypeMsg',
                                         'hits.hits._source.coordinates', 'hits.hits._source.averageRating',
                                         'hits.hits._source.storeCategory',
                                         'hits.hits._source.areaName',
                                         'hits.hits._source.storeType','hits.hits._source.storeIsOpen',
                                         'hits.hits._source.nextCloseTime', 'hits.hits._source.nextOpenTime'])


            if len(res) <= 0:
                # storeList = []
                for k in set_filterids_responseJson:
                    checkStorename_query = {"from": 0, "query": {
                        "bool": {"must": [{"match": {"status": 1}}, {"match": {"serviceZones": zoneId}},
                                          {"match": {"storeType": storeType}},
                                          {"match": {"_id": str(k)}}]}}, "size": 10}
                    storeList = []
                    res_store = es.search(index=index_store, doc_type=doc_type_store, body=checkStorename_query,
                                    filter_path=['hits.hits._id', 'hits.hits._source.sName',
                                                 'hits.hits._source.storeAddr',
                                                 'hits.hits._source.storeSubCategory', 'hits.hits._source.foodTypeName',
                                                 'hits.hits._source.foodType',
                                                 'hits.hits._source.cartsAllowed',
                                                 'hits.hits._source.costForTwo', 'hits.hits._source.storeDescription',
                                                 'hits.hits._source.profileLogos', 'hits.hits._source.bannerLogos',
                                                 'hits.hits._source.storeBillingAddr',
                                                 'hits.hits._source.franchiseId', 'hits.hits._source.franchiseName',
                                                 'hits.hits._source.minimumOrder',
                                                 'hits.hits._source.freeDeliveryAbove',
                                                 'hits.hits._source.storeType', 'hits.hits._source.storeTypeMsg',
                                                 'hits.hits._source.coordinates', 'hits.hits._source.averageRating',
                                                 'hits.hits._source.storeCategory',
                                                 'hits.hits._source.areaName',
                                                 'hits.hits._source.storeType', 'hits.hits._source.storeIsOpen',
                                                 'hits.hits._source.nextCloseTime', 'hits.hits._source.nextOpenTime'])

                    if len(res_store) <= 0:
                        storeList = []
                    else:
                        for i in res_store['hits']['hits']:
                            coords_1 = (
                                i['_source']['coordinates']['latitude'], i['_source']['coordinates']['longitude'])
                            coords_2 = (request.META['HTTP_LATITUDE'], request.META['HTTP_LONGITUDE'])
                            cat = [x['subCategoryName'][language] for x in i['_source']['storeSubCategory']] if len(i['_source']['storeSubCategory']) > 0 else [""]
                            storeData.append({
                                "businessId": i['_id'],
                                "storeAddr": i['_source']['storeAddr'],
                                "cartsAllowed": i['_source']['cartsAllowed'],
                                "storeBillingAddr": i['_source']['storeBillingAddr'] if 'storeBillingAddr' in i['_source'] else "",
                                "franchiseId": i['_source']['franchiseId'] if 'franchiseId' in i['_source'] else "",
                                "averageRating": i['_source']['averageRating'] if 'averageRating' in i['_source'] else "",
                                "franchiseName": i['_source']['franchiseName'] if 'franchiseName' in i['_source'] else "",
                                "minimumOrder": i['_source']['minimumOrder'] if 'minimumOrder' in i['_source'] else "",
                                "freeDeliveryAbove": i['_source']['freeDeliveryAbove'] if 'freeDeliveryAbove' in i['_source'] else "",
                                "storeType": i['_source']['storeType'] if 'storeType' in i['_source'] else "",
                                "areaName": i['_source']['areaName'] if 'areaName' in i['_source'] else "",
                                "storeTypeMsg": i['_source']['storeTypeMsg'] if 'storeTypeMsg' in i['_source'] else "",
                                "businessName": i['_source']['sName'][language] if 'sName' in i['_source'] else "",
                                "storeDescription": i['_source']['description'] if 'description' in i['_source'] else "",
                                'lat': i['_source']['coordinates']['latitude'],
                                'lng': i['_source']['coordinates']['longitude'],
                                "storeSubCategory": cat[0],
                                'storeIsOpen': i['_source']['storeIsOpen'],
                                'nextOpenTime':i['_source']['nextOpenTime'],
                                'nextCloseTime':i['_source']['nextCloseTime'],
                                "distance": geopy.distance.vincenty(coords_1, coords_2).km,
                                "foodTypeName": i['_source']['foodTypeName'] if 'foodTypeName' in i['_source'] else "",
                                "costForTwo": i['_source']['costForTwo'] if 'costForTwo' in i['_source'] else "",
                                "businessImage": i['_source']['profileLogos']['logoImage'] if 'profileLogos' in i['_source'] else "",
                                "bannerimage": i['_source']['bannerLogos']['bannerimage'] if 'bannerLogos' in i['_source'] else ""
                            })
            else:
                for i in res['hits']['hits']:
                    coords_1 = (
                        i['_source']['coordinates']['latitude'], i['_source']['coordinates']['longitude'])
                    coords_2 = (request.META['HTTP_LATITUDE'], request.META['HTTP_LONGITUDE'])
                    cat = [x['subCategoryName'][language] for x in i['_source']['storeSubCategory']] if len(i['_source']['storeSubCategory']) > 0 else [""]
                    storeData.append({
                        "businessId": i['_id'],
                        "storeAddr": i['_source']['storeAddr'],
                        "cartsAllowed": i['_source']['cartsAllowed'],
                        "storeBillingAddr": i['_source']['storeBillingAddr'] if 'storeBillingAddr' in i['_source'] else "",
                        "franchiseId": i['_source']['franchiseId'] if 'franchiseId' in i['_source'] else "",
                        "averageRating": i['_source']['averageRating'] if 'averageRating' in i['_source'] else "",
                        "franchiseName": i['_source']['franchiseName'] if 'franchiseName' in i['_source'] else "",
                        "minimumOrder": i['_source']['minimumOrder'] if 'minimumOrder' in i['_source'] else "",
                        "freeDeliveryAbove": i['_source']['freeDeliveryAbove'] if 'freeDeliveryAbove' in i['_source'] else "",
                        "storeType": i['_source']['storeType'] if 'storeType' in i['_source'] else "",
                        "areaName": i['_source']['areaName'] if 'areaName' in i['_source'] else "",
                        "storeTypeMsg": i['_source']['storeTypeMsg'] if 'storeTypeMsg' in i['_source'] else "",
                        "businessName": i['_source']['sName'][language] if 'sName' in i['_source'] else "",
                        "storeDescription": i['_source']['description'] if 'description' in i['_source'] else "",
                        'lat': i['_source']['coordinates']['latitude'],
                        'lng': i['_source']['coordinates']['longitude'],
                        "storeSubCategory": cat[0],
                        'storeIsOpen': i['_source']['storeIsOpen'],
                        'nextOpenTime':i['_source']['nextOpenTime'],
                        'nextCloseTime':i['_source']['nextCloseTime'],
                        "distance": geopy.distance.vincenty(coords_1, coords_2).km,
                        "foodTypeName": i['_source']['foodTypeName'] if 'foodTypeName' in i['_source'] else "",
                        "costForTwo": i['_source']['costForTwo'] if 'costForTwo' in i['_source'] else "",
                        "businessImage": i['_source']['profileLogos']['logoImage'] if 'profileLogos' in i['_source'] else "",
                        "bannerimage": i['_source']['bannerLogos']['bannerimage'] if 'bannerLogos' in i['_source'] else ""
                    })
            dataframe = pd.DataFrame(storeData)
            dataframe = dataframe.drop_duplicates(subset='businessId', keep="last")
            dataframe["distanceMiles"] = dataframe.apply(storeDistance, latitude=request.META['HTTP_LATITUDE'], longtitude=request.META['HTTP_LONGITUDE'], axis=1)
            dataframe["distanceKm"] = dataframe.apply(storeDistanceKm, latitude=request.META['HTTP_LATITUDE'], longtitude=request.META['HTTP_LONGITUDE'], axis=1)
            dataframe["distance"] = dataframe.apply(storeDistanceKm, latitude=request.META['HTTP_LATITUDE'], longtitude=request.META['HTTP_LONGITUDE'], axis=1)
            # dataframe['storetimeQuery'] = dataframe.apply(storeQuery, axis=1)
            details = dataframe.to_json(orient='records')
            data = json.loads(details)
            for j in data:
                storeList.append(j)
            finalSuggestions['resturantData'] = storeList
            print(finalSuggestions)
            print("total time taken", time.time()-start)
            return JsonResponse(finalSuggestions, safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return JsonResponse(error, status=404)





async def zone_wise_search_read(res, start_time, language, filter_responseJson, finalfilter_responseJson_stores,
                      finalfilter_responseJson_products, popularstatus):
    try:
        currdate = datetime.datetime.now()
        eastern = timezone(timezonename)
        currlocal = eastern.localize(currdate)
        currlocaltimestamp = currlocal.timestamp()
        currlocalISO = datetime.datetime.fromtimestamp(currlocaltimestamp)

        if len(res) <= 0:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return error

        else:
            for i in res['hits']['hits']:
                unitdata = []
                if 'units' in i['_source']:
                    for j in i['_source']['units']:
                        if 'floatValue' in j:
                            floatprice = j['floatValue']
                        else:
                            floatprice = "none"

                        unitdata.append({
                            "unitName": j['name'][language],
                            "unitId": j['unitId'],
                            "unitPrice": j['price'][language],
                            "floatunitPrice": floatprice,
                        })
                if 'offer' in i['_source']:
                    print(i['_id'])
                    if len(i["_source"]["offer"]) > 0:
                        for j in i["_source"]["offer"]:
                            if j['status'] == 1 and float(j['endDateTime']) > currlocaltimestamp:
                                print("Offer Active")
                                pricedata = []
                                if j['discountType'] == 0:
                                    print('dis 0')
                                    discount_value = j['discountValue']
                                    for k in unitdata:
                                        pricedata.append({
                                            "unitId": k['unitId'],
                                            "unitPrice": k['unitPrice'],
                                            "finalPrice": float(k['unitPrice']) - float(discount_value),
                                            "discount_value": discount_value
                                        })
                                elif j['discountType'] == 1:
                                    print('dis 1')
                                    for k in unitdata:
                                        discount_value = (float(k['unitPrice']) * float(
                                            j['discountValue'])) / 100
                                        pricedata.append({
                                            "unitId": k['unitId'],
                                            "unitPrice": k['unitPrice'],
                                            "finalPrice": float(k['unitPrice']) - discount_value,
                                            "discount_value": discount_value

                                        })

                                if popularstatus == '0':
                                    print('popular 0')
                                    filter_responseJson.append({
                                        "childProductId": i['_id'],
                                        "productName": i['_source']['productname'][language],
                                        "parentProductId": i['_source']['parentProductId'],
                                        "sku": i['_source']['sku'],
                                        "CBD": i['_source']['CBD'],
                                        "THC": i['_source']['THC'],
                                        "units": unitdata,
                                        "storeId": i['_source']['storeId'],
                                        "storeName": i['_source']['store'][language],
                                        "mobileImage": i['_source']['images'],
                                        "finalPriceList": pricedata,
                                        "offerId": j['offerId']
                                    })
                                else:
                                    print('popular 1')
                                    filter_responseJson.append({
                                        "childProductId": i['_source']['childProductId'],
                                        "productName": i['_source']['productname'][language],
                                        # "parentProductId": i['_source']['parentProductId'],
                                        # "sku": i['_source']['sku'],
                                        # "CBD": i['_source']['CBD'],
                                        # "THC": i['_source']['THC'],
                                        "units": unitdata,
                                        "storeId": i['_source']['storeId'],
                                        # "storeName": i['_source']['store'][language],
                                        "mobileImage": i['_source']['images'],
                                        "finalPriceList": pricedata,
                                        "offerId": j['offerId']
                                    })
                            elif j['status'] == 3 or float(j['endDateTime']) < currlocaltimestamp:
                                print("No Offer else")
                                filter_responseJson.append({
                                    "childProductId": i['_id'],
                                    "productName": i['_source']['productname'][language],
                                    "parentProductId": i['_source']['parentProductId'],
                                    "sku": i['_source']['sku'],
                                    "CBD": i['_source']['CBD'],
                                    "THC": i['_source']['THC'],
                                    "units": unitdata,
                                    "storeId": i['_source']['storeId'],
                                    "storeName": i['_source']['store'][language],
                                    "mobileImage": i['_source']['images']
                                })
                    else:
                        print("No Offer in collection list")
                        if popularstatus == '0':
                            filter_responseJson.append({
                                "childProductId": i['_id'],
                                "productName": i['_source']['productname'][language],
                                "parentProductId": i['_source']['parentProductId'],
                                "sku": i['_source']['sku'],
                                "CBD": i['_source']['CBD'],
                                "THC": i['_source']['THC'],
                                "units": unitdata,
                                "storeId": i['_source']['storeId'],
                                "storeName": i['_source']['store'][language],
                                "mobileImage": i['_source']['images']
                            })
                        else:
                            filter_responseJson.append({
                                "childProductId": i['_source']['childProductId'],
                                "productName": i['_source']['productname'][language],
                                # "parentProductId": i['_source']['parentProductId'],
                                # "sku": i['_source']['sku'],
                                # "CBD": i['_source']['CBD'],
                                # "THC": i['_source']['THC'],
                                "units": unitdata,
                                "storeId": i['_source']['storeId'],
                                # "storeName": i['_source']['store'][language],
                                "mobileImage": i['_source']['images']
                            })

                else:
                    print("No Offer in collection")
                    print(i)
                    if popularstatus == '0':
                        filter_responseJson.append({
                            "childProductId": i['_id'],
                            "productName": i['_source']['productname'][language],
                            "parentProductId": i['_source']['parentProductId'],
                            "sku": i['_source']['sku'],
                            "CBD": i['_source']['CBD'],
                            "THC": i['_source']['THC'],
                            "units": unitdata,
                            "storeId": i['_source']['storeId'],
                            "storeName": i['_source']['store'][language],
                            "mobileImage": i['_source']['images']
                        })
                    else:
                        filter_responseJson.append({
                            "childProductId": i['_id'],
                            "productName": i['_source']['productname'][language],
                            "parentProductId": i['_source']['parentProductId'],
                            "sku": i['_source']['sku'],
                            "CBD": i['_source']['CBD'],
                            "THC": i['_source']['THC'],
                            "units": unitdata,
                            "storeId": i['_source']['storeId'],
                            "storeName": i['_source']['store'][language],
                            "mobileImage": i['_source']['images']
                        })
            print("parsing stage 1 end", time.time() - start_time)

            loopdata = True
            while (loopdata == True):
                finaljson = []
                for i in filter_responseJson:
                    finaljson.append(i['childProductId'])
                index_count = []
                for i in finaljson:
                    if finaljson.count(i) > 1:
                        for j in filter_responseJson:
                            if j['childProductId'] == i:
                                index_count.append(finaljson.index(i))

                for i in set(index_count):
                    print("1")
                    for j in filter_responseJson:
                        print("2")
                        if j['childProductId'] == finaljson[int(i)]:
                            print("3")
                            if 'offerId' in j:
                                print("offer")
                            else:
                                print("no offer")
                                del filter_responseJson[int(i)]
                if len(set(index_count)) == 0:
                    loopdata = False

            # print(filter_responseJson)

            for i in filter_responseJson:
                finalfilter_responseJson_products.append(i)
                serarchResults_products = {
                    "products": finalfilter_responseJson_products
                }

            if len(finalfilter_responseJson_products) != 0:
                finalSearchResults = {
                    "data": serarchResults_products,
                    "message": "Got the details"
                }
            else:
                finalSearchResults = {
                    "data": [],
                    "message": "No Data Found"
                }

        print("parsing stage 2 end", time.time() - start_time)
        # print(finalSearchResults)
        return finalSearchResults
    except:
        error = {
            "data": [],
            "message": "Internal Error search_read"
        }
        return JsonResponse(error, status=500)



async def zone_wise_popular_search_write(pname, res_popular_test, start_time, zid, language):
    try:
        print("*********************")
        currdate = datetime.datetime.now()
        eastern = timezone(timezonename)
        currlocal = eastern.localize(currdate)
        currlocaltimestamp = currlocal.timestamp()
        currlocalISO = datetime.datetime.fromtimestamp(currlocaltimestamp)

        temp_count = 0
        if len(res_popular_test) == 0:
            print("*********if************")

            Poular_search_data = {
                "storeId": "",
                "zoneId": zid,
                "productName": pname,
                "search_timestamp": currlocaltimestamp,
                "count": 0
            }
            res_popular = es.index(index=index_popularSearch, doc_type=doc_type_popularSearch, body=Poular_search_data)
            print(res_popular)
            print("*********if Added************")

        else:
            print("*********else************")

            for i in res_popular_test['hits']['hits']:
                if i['_source']['productName'] == pname:
                    print("data matched")
                    temp_count += 1
                    iddata = i['_id']
                    final_count = i['_source']['count']

                else:
                    print("*********if else************")

                    print("data not found")

            if temp_count == 0:
                print("*********if if************")

                Poular_search_data = {
                    "storeId": "",
                    "zoneId": zid,
                    "productName": pname,
                    "search_timestamp": currlocaltimestamp,
                    "count": 0
                }
                res_popular = es.index(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                       body=Poular_search_data)
                print(res_popular)
            else:
                final_count_final = int(final_count) + 1
                res_pop_update = es.update(index=index_popularSearch, doc_type=doc_type_popularSearch, id=iddata,
                                           body={"doc": {"count": final_count_final}})
                print(res_pop_update)
                print("*********else if else Added************")

        print("end", time.time() - start_time)
    except:
        error = {
            "data": [],
            "message": "Internal Error popular_search_write"
        }
        return JsonResponse(error, status=500)



class ZoneWiseSearchFilter(APIView):
    def post(self, request):
        try:
            start_time = time.time()
            print(start_time)
            finalfilter_responseJson_products = []
            finalfilter_responseJson_stores = []
            filter_responseJson = []
            offerJson = []
            print(request.META)
            language = request.META['HTTP_LANGUAGE']
            zoneId = request.META['HTTP_ZONEID']
            if 'HTTP_SEARCHEDITEM' in request.META:
                searchedItem = request.META['HTTP_SEARCHEDITEM']

            # es read start
            search_item_query = request.data

            if request.META['HTTP_POPULARSTATUS'] == '0':

                print("Es Query start", time.time() - start_time)
                print(search_item_query)

                res = es.search(index=index_products, doc_type=doc_type_products, body=search_item_query,
                                filter_path=['hits.hits._id', 'hits.hits._source.CBD', 'hits.hits._source.THC',
                                             'hits.hits._source.images', 'hits.hits._source.parentProductId',
                                             'hits.hits._source.productname', 'hits.hits._source.sku',
                                             'hits.hits._source.storeId', 'hits.hits._source.store',
                                             'hits.hits._source.units',
                                             'hits.hits._source.offer'])

                # print(res)
                print("Es Query end", time.time() - start_time)
                if len(res) <= 0:
                    final_json = {
                        "data": [],
                        "message": "No Data Found",
                    }
                    return JsonResponse(final_json, safe=False, status=404)

                # es read end

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                        "query": {
                            "bool": {
                                "must": [{"match": {"productName": searchedItem}}]
                            }
                        }
                    }
                    print(popular_search_query)
                    res_popular_test = es.search(index=index_products, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    print("Es Query end", time.time() - start_time)
                    print(res_popular_test)
                    # es write end
                    data = loop.run_until_complete(asyncio.gather(
                        zone_wise_search_read(res, start_time, language, filter_responseJson,
                                     finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS']),
                        zone_wise_popular_search_write(searchedItem, res_popular_test, start_time,
                                             zoneId, language)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        zone_wise_search_read(res, start_time, language, filter_responseJson,
                                     finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'])))
                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)
            else:
                print("Es Query start", time.time() - start_time)
                print(search_item_query)

                res = es.search(index=index_trendingProducts, doc_type=doc_type_trendingProducts, body=search_item_query,
                                filter_path=['hits.hits._id',
                                             'hits.hits._source.childProductId',
                                             'hits.hits._source.productname',
                                             'hits.hits._source.storeId',
                                             'hits.hits._source.units',
                                             'hits.hits._source.offer', 'hits.hits._source.images'])

                # print(res)
                print("Es Query end", time.time() - start_time)
                # es read end

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                        "query": {
                            "bool": {
                                "must": [{"match": {"productName": searchedItem}}]
                            }
                        }
                    }
                    print(popular_search_query)
                    res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    print("Es Query end", time.time() - start_time)
                    print(res_popular_test)
                    # es write end
                    data = loop.run_until_complete(asyncio.gather(
                        zone_wise_search_read(res, start_time, language, filter_responseJson,
                                     finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS']),
                        zone_wise_popular_search_write(searchedItem, res_popular_test, start_time,
                                             zoneId, language)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        zone_wise_search_read(res, start_time, language, filter_responseJson,
                                     finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'])))
                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)
        except:
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error,safe=False, status=500)


class cities(APIView):
    def get(self, request):
        try:
            client = MongoClient(mongo_url)
            db2 = client[mongo_db]
            cities_json = []
            cities = db2.cities.find({})
            for i in cities:
                for j in i['cities']:
                    cities_json.append(j)
            df_cities = pd.DataFrame(list(cities_json))
            df_cities_data = df_cities[
                ['cityId', 'cityName', 'abbrevation', 'abbrevationText', 'currency', 'currencySymbol']]
            df_cities_jsondata = df_cities_data.to_dict('records')
            final_cities = json.loads(json_util.dumps(df_cities_jsondata))
            return JsonResponse(final_cities, safe=False)
        except:
            error_message = {
                "error": "Invalid request"
            }
            return JsonResponse(error_message, status=500)


async def filter_read(res, start_time, language, filter_responseJson, sname, finalfilter_responseJson_stores,
                      finalfilter_responseJson_products, popularstatus, sort):
    try:
        currdate = datetime.datetime.now()
        addOns = []
        addOnAvailable = "0"
        eastern = timezone(timezonename)
        currlocal = eastern.localize(currdate)
        currlocaltimestamp = currlocal.timestamp()
        currlocalISO = datetime.datetime.fromtimestamp(currlocaltimestamp)
        resData = []
        start = time.time()
        if len(res) <= 0:
            error = {
                "data": [],
                "message": "No Products Found"
            }
            return error
        else:
            catData = []
            for i in res['hits']['hits']:
                addOns = []
                addOnsData = []
                currencySymbol = i['_source']['currencySymbol'] if "currencySymbol" in i['_source'] else "₹"
                currency = i['_source']['currency'] if "currency" in i['_source'] else "INR"
                catData.append({
                    "name": i['_source']['subSubCategoryName'][0],
                    "id": i['_source']['thirdCategoryId']
                })
                if 'addOns' in i['_source']:
                    if len(['addOns']) > 0:
                        addOnAvailable = 1
                        for j in i['_source']['addOns']:
                            for z in j['addOns']:
                                addOns.append({
                                    "id": z["id"],
                                    "name": z["name"],
                                    "price": z['price'],
                                    "storeAddOnId": z['storeAddOnId']
                                })
                            addOnsData.append({
                                'addOnLimit': j['addOnLimit'] if 'addOnLimit' in j else 0,
                                'addOns': addOns,
                                'id': j['id'],
                                'mandatory': j['mandatory'],
                                'multiple': j['multiple'] if 'multiple' in j else 0,
                                'name': j['name'],
                                'description': j['description'] if 'description' in j else {}
                            })
                    else:
                        addOnAvailable = 0
                        addOnsData = []
                else:
                    addOnAvailable = 0
                    addOnsData = []
                resData.append({
                    "childProductId": i['_id'],
                    "categoryId": i['_source']['firstCategoryId'],
                    "subCategoryId": i['_source']['secondCategoryId'],
                    "subSubCategoryId": i['_source']['thirdCategoryId'],
                    "subCategoryName": i['_source']['subCategoryName'],
                    "subSubCategoryName": i['_source']['subSubCategoryName'],
                    "productName": i['_source']['productname'][language],
                    "parentProductId": i['_source']['parentProductId'],
                    "sku": i['_source']['sku'],
                    "CBD": i['_source']['CBD'],
                    "THC": i['_source']['THC'],
                    "currencySymbol": currencySymbol,
                    "currency": currency,
                    "storeId": i['_source']['storeId'],
                    "storeName": i['_source']['store'][language],
                    "mobileImage": i['_source']['images'],
                    "units": i['_source']['units'] if 'units' in i['_source'] else [],
                    "offer": i['_source']['offer'] if 'offer' in i['_source'] else [],
                    "popularstatus": popularstatus,
                    "addOnAvailable": addOnAvailable,
                    "addOns": addOnsData
                })
            dataframe = pd.DataFrame(resData)
            dataframe["popularstatus"] = popularstatus
            dataframe["unitsData"] = dataframe.apply(unitsData, lan=language, curntime=currlocaltimestamp, sort=sort, axis=1)
            dataframe = dataframe.drop_duplicates(subset='childProductId', keep="last")
            details = dataframe.to_json(orient='records')
            data = json.loads(details)
            cat_data = list({v['id']:v for v in catData}.values())
            for cat in cat_data:
                categoty_data = []
                for k in data:
                    if k['subSubCategoryName'][0] == cat['name']:
                        for q in k['unitsData']:
                            if 'availableQuantity' in q:
                                if q['availableQuantity'] > 0:
                                    outOfStock = False
                                    availableQuantity = q['availableQuantity']
                                else:
                                    outOfStock = True
                                    availableQuantity = 0
                            else:
                                outOfStock = True
                                availableQuantity = 0
                        categoty_data.append({
                            "outOfStock": outOfStock,
                            "childProductId": k['childProductId'],
                            "productName": k['productName'],
                            "parentProductId": k['parentProductId'],
                            "sku": k['sku'],
                            "CBD": k['CBD'],
                            "availableQuantity": availableQuantity,
                            "THC": k['THC'],
                            "units": k['unitsData'],
                            "storeId": k['storeId'],
                            "storeName": k['storeName'],
                            "mobileImage": k['mobileImage'],
                            "finalPriceList": k['unitsData'],
                            "offerId": "",
                            "currencySymbol": k['currencySymbol'],
                            "currency": k['currency'],
                            "addOnAvailable": k['addOnAvailable'],
                            "addOns": k['addOns']
                        })
                filter_responseJson.append({
                    "subCategoryId": k['categoryId'],
                    "categoryId": k['categoryId'],
                    "subSubCategoryName": cat['name'],
                    "subSubCategoryId": cat['id'],
                    "products": categoty_data,
                    "storeId": k['storeId'],
                    "storeName": k['storeName'],
                })
            print("parsing stage 1 end", time.time() - start)

            for i in filter_responseJson:
                if i['storeId'] == sname:
                    finalfilter_responseJson_products.append(i)
                    serarchResults_products = {
                        "subSubCategories": finalfilter_responseJson_products
                    }
                elif sname == str(0):
                    finalfilter_responseJson_products.append(i)
                    serarchResults_products = {
                        "subSubCategories": finalfilter_responseJson_products
                    }


            if len(finalfilter_responseJson_products) != 0:
                finalSearchResults = {
                    "data": serarchResults_products,
                    "message": "Got the details"
                }
            else:
                store_details = []
                storeIDS = []
                for i in serarchResults_stores['stores']:
                    storeIDS.append(i["storeId"])
                for i in list(set(storeIDS)):
                    storeId_store, storeLogo_store, storeAddress_store, storeName_store, storeSubCategory, \
                    averageRating,latitude , longitude, storedescription , \
                    avgdeliverytime, costForTwo = getStoresDetails(i, language)
                    if storeId_store == 0:
                        serarchResults_products = []
                    else:
                        store_details.append(
                            {
                                "businessImage": storeLogo_store,
                                "businessAddress": storeAddress_store,
                                "businessId": storeId_store,
                                "businessName": storeName_store,
                                "averageRating": averageRating,
                                "costForTwo": costForTwo,
                                "latitude": latitude,
                                "longitude": longitude,
                                "estimatedtime": avgdeliverytime,
                                "storeMessage": storedescription
                            }
                        )

                    serarchResults_products = {
                        "stores": store_details
                    }
                    finalSearchResults = {
                        "data": serarchResults_products,
                        "message": "Got the details"
                    }
            print("parsing stage 2 end", time.time() - start)
            print("total time taken", time.time()-start)
            finalSearchResults = {
                "data": serarchResults_products,
                "message": "Got the details"
            }
            return finalSearchResults
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        error = {
            "data": [],
            "message": "Internal Error search_read"
        }
        return JsonResponse(error, status=500)


'''
    API for the getting the filter data category wise
'''
class FilterProducts(APIView):
    def post(self, request):
        try:
            start_time = time.time()
            finalfilter_responseJson_products = []
            finalfilter_responseJson_stores = []
            filter_responseJson = []
            offerJson = []
            language = request.META['HTTP_LANGUAGE']
            storeId = request.META['HTTP_STOREID']
            zoneId = request.META['HTTP_ZONEID']
            if storeId == "0":
                checkoffers = requests.get(SERVER_OFFER + 'offerslist/' + str(zoneId) + "/" + str(0),
                                           headers={"authorization": request.META['HTTP_AUTHORIZATION'],
                                                    "language": language})
            else:
                checkoffers = requests.get(SERVER_OFFER + 'offerslist/' + str(zoneId) + "/" + str(storeId),
                                           headers={"authorization": request.META['HTTP_AUTHORIZATION'],
                                                    "language": language})

            if 'HTTP_SEARCHEDITEM' in request.META:
                searchedItem = request.META['HTTP_SEARCHEDITEM']

            search_item_query = request.data
            print("===============", search_item_query)

            if type(search_item_query) == str or type(search_item_query) == '':
                search_item_query = json.loads(search_item_query)

            if 'sort' in search_item_query:
                if 'units.floatValue' in search_item_query['sort']:
                    if search_item_query['sort']['units.floatValue']['order'] == "asc":
                        sort = 0
                    elif search_item_query['sort']['units.floatValue']['order'] == "desc":
                        sort = 1
                else:
                    sort = 2
            else:
                sort = 3
            if request.META['HTTP_POPULARSTATUS'] == '0':
                if storeId != '0':
                    for z in search_item_query['query']['bool']['must']:
                        if 'match' in z:
                            if 'zoneId' in z['match']:
                                z['match']['storeId'] = z['match'].pop('zoneId')
                                z['match']['storeId'] = storeId
                            else:
                                pass

                res = es.search(index=index_products, doc_type=doc_type_products, body=search_item_query,
                                filter_path=['hits.hits._id', 'hits.hits._source.CBD', 'hits.hits._source.THC',
                                             'hits.hits._source.images', 'hits.hits._source.parentProductId',
                                             'hits.hits._source.productname', 'hits.hits._source.sku',
                                             'hits.hits._source.storeId', 'hits.hits._source.store',
                                             'hits.hits._source.units',
                                             'hits.hits._source.firstCategoryId',
                                             'hits.hits._source.secondCategoryId',
                                             'hits.hits._source.thirdCategoryId',
                                             'hits.hits._source.categoryName',
                                             'hits.hits._source.subCategoryName',
                                             'hits.hits._source.subSubCategoryName',
                                             'hits.hits._source.addOns',
                                             'hits.hits._source.currencySymbol', 'hits.hits._source.currency',
                                             'hits.hits._source.offer'])
                if len(res) <= 0:
                    final_json = {
                        "data": [],
                        "message": "No Data Found",
                    }
                    return JsonResponse(final_json, safe=False, status=404)

                # es read end

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                      "query": {
                        "match_phrase_prefix": {"productName": searchedItem}
                      }
                    }
                    res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    res_popular_result = es.search(index=index_products, doc_type=doc_type_products,
                                                 body=search_item_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])

                    # es write end
                    data = loop.run_until_complete(asyncio.gather(
                        filter_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort),
                        popular_search_write(searchedItem, res_popular_test, start_time, storeId,
                                             zoneId, language, request.META['HTTP_STORECATEGORYID'],
                                             request.META['HTTP_STORETYPE'], res_popular_result)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        filter_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))

                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)

            elif request.META['HTTP_POPULARSTATUS'] == '2':
                if type(search_item_query) == str or type(search_item_query) == '':
                    search_item_query = json.loads(search_item_query)

                if storeId != '0':
                    for z in search_item_query['query']['bool']['must']:
                        if 'match' in z:
                            if 'zoneId' in z['match']:
                                z['match']['storeId'] = z['match'].pop('zoneId')
                                z['match']['storeId'] = storeId
                            else:
                                pass
                res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                             body=search_item_query,
                                             filter_path=['hits.hits._id', 'hits.hits._source'])

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)

                data = loop.run_until_complete(asyncio.gather(
                    filter_read(res_popular_test, start_time, language, filter_responseJson,
                                storeId, finalfilter_responseJson_stores,
                                finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))
                loop.close()
                if len(data[0]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)

            else:
                print("----------------------------for popular 1------------------------------------------------------")
                print("Es Query start", time.time() - start_time)
                print(search_item_query)

                res = es.search(index=index_trendingProducts, doc_type=doc_type_trendingProducts, body=search_item_query,
                                filter_path=['hits.hits._id',
                                             'hits.hits._source.childProductId',
                                             'hits.hits._source.productname',
                                             'hits.hits._source.storeId',
                                             'hits.hits._source.units',
                                             'hits.hits._source.offer', 'hits.hits._source.images'])

                print("Es Query end", time.time() - start_time)
                # es read end

                loop = asyncio.new_event_loop()
                event_loop = asyncio.set_event_loop(loop)
                if "HTTP_SEARCHEDITEM" in request.META:
                    popular_search_query = {
                        "query": {
                            "bool": {
                                "must": [{"match": {"productName": searchedItem}}]
                            }
                        }
                    }
                    print(popular_search_query)
                    res_popular_test = es.search(index=index_popularSearch, doc_type=doc_type_popularSearch,
                                                 body=popular_search_query,
                                                 filter_path=['hits.hits._id', 'hits.hits._source'])


                    print("Es Query end", time.time() - start_time)
                    data = loop.run_until_complete(asyncio.gather(
                        filter_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort),
                        popular_search_write(searchedItem, res_popular_test, start_time, storeId,
                                             zoneId, language, res_popular_result, res_popular_test)))
                    loop.close()
                else:
                    data = loop.run_until_complete(asyncio.gather(
                        filter_read(res, start_time, language, filter_responseJson,
                                    storeId, finalfilter_responseJson_stores,
                                    finalfilter_responseJson_products, request.META['HTTP_POPULARSTATUS'], sort)))
                if len(data[0]["data"]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    return JsonResponse(data[0], safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error,safe=False, status=500)


'''
    API for the get the offer details from the db
'''
class OffersDetails(APIView):
    def post(self, request):
        try:
            offer_data = []
            data = request.data
            lan = request.META['HTTP_LANGUAGE']
            res = es.search(index=index_offers, doc_type=doc_type_offers, body=data,
                            filter_path=['hits.hits._id',
                                         'hits.hits._source.offername',
                                         'hits.hits._source.offerdescription',
                                         'hits.hits._source.images',
                                         'hits.hits._source.storeType',
                                         'hits.hits._source.currencySymbol',
                                         'hits.hits._source.currency',
                                         'hits.hits._source.description',
                                         'hits.hits._source.storeId',
                                         'hits.hits._source.StoreProfileLogo',
                                         'hits.hits._source.StoreBannerLogos',
                                         'hits.hits._source.minimumPurchaseQty',
                                         'hits.hits._source.sName',
                                         'hits.hits._source.discountValue'])
            if len(res) > 0:
                for i in res['hits']['hits']:
                    # stores_details = db.stores.find_one({"status": 1, "_id": ObjectId(i['_source']['storeId'])},
                    #                             {"_id": 1, "addressCompo": 1, "areaName": 1, "cartsAllowed": 1})
                    store_query = {"from": 0, "size": 10, "query": {"bool": {"must": [{"match": {"status": 1}}, {"match": {"_id": str(i['_source']['storeId'])}}]}}}
                    store_res = es.search(index=index_store, doc_type=doc_type_store, body=store_query,
                                    filter_path=['hits.hits._id',
                                                 'hits.hits._source.addressCompo',
                                                 'hits.hits._source.areaName',
                                                 'hits.hits._source.cartsAllowed'])
                    if len(store_res) > 0:
                        print(store_res)
                        for j in store_res['hits']['hits']:
                            address_compo = j['_source']['addressCompo']
                            area_name = j['_source']['areaName']
                            carts_allowed = j['_source']['cartsAllowed']
                    else:
                        address_compo = ""
                        area_name = ""
                        carts_allowed = ""

                    offer_data.append({
                        "_id": str(i['_id']),
                        "addressCompo": address_compo,
                        "areaName": area_name,
                        "cartsAllowed": carts_allowed,
                        "name": i['_source']['offername'][lan],
                        "images": i['_source']['images'],
                        "description": i['_source']['offerdescription'][lan] if 'offerdescription' in i['_source'] else "",
                        "storeId": i['_source']['storeId'],
                        "storeType": i['_source']['storeType'],
                        "currencySymbol": i['_source']['currencySymbol'],
                        "currency": i['_source']['currency'],
                        "storeLogo": i['_source']['StoreProfileLogo'],
                        "storeBannerLogo": i['_source']['StoreBannerLogos'],
                        "storeName": i['_source']['sName'][lan],
                    })
                response = {
                    "message": "Data Found",
                    "data": offer_data
                }
                return JsonResponse(response, safe=False, status=200)
            else:

                response = {
                    "message": "Data Found",
                    "data": offer_data
                }
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            error = {
                "data": [],
                "message": "Internal Error"
            }
            return JsonResponse(error,safe=False, status=500)