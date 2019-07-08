import datetime
from django.test import TestCase
from django.utils import timezone
from django.test import Client
from django.urls import reverse
import json
client = Client()

class LanguageList(TestCase):
    def test_get_language_found(self):
        """
        If language exist more than one default language are english
        """
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 2")

    def test_get_language_not_found(self):
        """
        if language not found from the data base
        """
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 1")

    def test_get_language_internal_server(self):
        """
        If internal server occurs
        """
        response = self.client.get("/languageList/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 1")


class ProductSuggestions(TestCase):
    def test_get_suggestion_found(self):
        """
        If product suggestions found
        """
        json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/suggestions/", json=json_data,HTTP_LANGUAGE="en",
                                    HTTP_ZONEID="5d08c6e6087d92283a7f9634")
        print(response.json())
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()["data"]), ">= 0")

    def test_get_suggestion_internal(self):
        """
        If products not found and internal server error
        """
        json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
        response = self.client.post("/suggestions/", json=json_data,
                                    HTTP_LANGUAGE="pt", HTTP_ZONEID="5d08c6e6087d92283a7f9636")
        self.assertEqual(response.status_code, 404)


# class FilterParameters(TestCase):
#     def test_get_suggestion_found(self):
#         """
#         If language exist more than one default language are english
#         """
#         json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
#         print(json_data)
#         response = self.client.post("/filterParameters/", json=json_data,HTTP_LANGUAGE="en",
#                                     HTTP_ZONEID="5d08c6e6087d92283a7f9634")
#         print(response.json())
#         self.assertEqual(response.status_code, 200)
#         self.assertTrue(len(response.json()["data"]), ">= 0")
#
#     def test_get_suggestion_internal(self):
#         """
#         If language exist more than one default language are english
#         """
#         json_data = {"from":0,"size":10,"query":{"bool":{"must":[{"match":{"status":1}},{"match_phrase_prefix":{"productname.en":"arep"}},{"match":{"storeId":"5cecda4b23c6d964170d5263"}}]}}}
#         response = self.client.post("/filterParameters/", json=json_data,
#                                     HTTP_LANGUAGE="en", HTTP_ZONEID="5d08c6e6087d92283a7f9634")
#         self.assertEqual(response.status_code, 404)
#         self.assertContains(response, "No Products Found")

