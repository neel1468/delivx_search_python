from symspellpy.symspellpy import SymSpell  # import the module
data = {'query': {'bool': {'must': [{'match': {'status': 1}}, {'match': {'serviceZones': '5b7bf3fbf801683f1b14b3f6'}}, {'match': {'storeCategory.categoryId': '5c5d9b76087d921152392c27'}}, {'match': {'storeType': 2}}, {'match_phrase_prefix': {'sName.en': 'kroge'}}]}}, 'from': 0, 'size': 10}
max_edit_distance_dictionary = 2
prefix_length = 7
sym_spell = SymSpell(max_edit_distance_dictionary, prefix_length)
dictionary_path = "/home/embed/searchandOffers/Delivx/ElasticSearch/ElasticSearch/dataJson"
term_index = 0
count_index = 1
if not sym_spell.load_dictionary(dictionary_path, term_index, count_index):
    print("Dictionary file not found")
for i in data['query']['bool']['must']:
    if 'match_phrase_prefix' in i:
        if 'sName.en' in i['match_phrase_prefix']:
            input_term = i['match_phrase_prefix']['sName.en']
            result = sym_spell.word_segmentation(input_term)
            i['match_phrase_prefix']['sName.en'] = i['match_phrase_prefix'].pop('sName.en')
            i['match_phrase_prefix']['sName.en'] = result.corrected_string
            print("{}".format(result.corrected_string))
    else:
        pass
print(data)
