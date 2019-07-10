import datetime
from django.test import TestCase
from django.utils import timezone
from django.test import Client
from django.urls import reverse
import json
client = Client()

class LanguageList(TestCase):
    def test_get_language_found(self):
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 2")

    def test_get_language_not_found(self):
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 1")

    def test_get_language_internal_server(self):
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 1")


# class ProductSuggestions(TestCase):
#     def test_get_suggestion_found(self):
#         json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
#         response = self.client.post("/suggestions/", json=json_data,HTTP_LANGUAGE="en",
#                                     HTTP_ZONEID="5d08c6e6087d92283a7f9634")
#         print(response.json())
#         self.assertEqual(response.status_code, 200)
#         self.assertTrue(len(response.json()["data"]), ">= 0")
#
#     def test_get_suggestion_internal(self):
#         json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
#         response = self.client.post("/suggestions/", json=json_data,
#                                     HTTP_LANGUAGE="pt", HTTP_ZONEID="5d08c6e6087d92283a7f9636")
#         self.assertEqual(response.status_code, 404)


class FilterParameters(TestCase):
    def test_get_filterparameter_manufacture_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="5",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_category_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="1",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_subcategory_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="2",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_subsubcategory_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="3",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_brand_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="4",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_price_found(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="5",HTTP_POPULARSTATUS="0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_filterparameter_category_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="1",HTTP_POPULARSTATUS="5")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_manufacture_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="5",HTTP_POPULARSTATUS="9")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_category_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="1",HTTP_POPULARSTATUS="5")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_subcategory_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="2",HTTP_POPULARSTATUS="5")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_subsubcategory_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="3",HTTP_POPULARSTATUS="5")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_brand_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="4",HTTP_POPULARSTATUS="4")
        self.assertEqual(response.status_code, 404)

    def test_get_filterparameter_price_error(self):
        json_data = {"from":1,"size":4,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"catName.en":"Refrigerated"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_FILTERTYPE="5",HTTP_POPULARSTATUS="4")
        self.assertEqual(response.status_code, 404)

