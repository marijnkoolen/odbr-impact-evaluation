import csv
import random
import re
from elasticsearch import Elasticsearch

class QuerySentenceSampler(object):

    def __init__(self, es_config, ignore_ids=[], debug=False):
        self.ignore_ids = ignore_ids
        self.debug = debug
        self.es = Elasticsearch()
        self.es_config = es_config

    def filter_test_sentences(self, hits):
        return [hit for hit in hits if hit["_source"]["review_id"] not in self.ignore_ids]

    def remove_duplicate_sentences(self, hits):
        sentences = []
        deduplicated_hits = []
        for hit in hits:
            if hit["_source"][self.es_config["analyzed_field"]] not in sentences:
                deduplicated_hits += [hit]
                sentences += [hit["_source"][self.es_config["analyzed_field"]]]
        return deduplicated_hits

    def search_and_filter(self, query):
        response = self.es.search(index=self.es_config["index"], doc_type=self.es_config["doc_type"], body=query)
        total = response['hits']['total']
        unfiltered = len(response['hits']['hits'])
        filtered_hits = self.filter_test_sentences(response['hits']['hits'])
        filtered = len(filtered_hits)
        deduplicated_hits = self.remove_duplicate_sentences(response['hits']['hits'])
        deduplicated = len(deduplicated_hits)
        if self.debug:
            print("returning hits total/unfiltered/filtered/deduplicated: {t}/{u}/{f}/{d}".format(t=total, u=unfiltered, f=filtered, d=deduplicated))
        return deduplicated_hits

    def make_regexp_query(self, regexp, field=None):
        if not field:
            field = self.es_config["regexp_field"]
        return {"regexp": {field: ".*{r}.*".format(r=regexp)}}

    def make_match_phrase_query(self, chunk, field=None):
        if not field:
            field = self.es_config["analyzed_field"]
        return {"match_phrase": {field: chunk}}

    def make_impact_phrase_query_chunk(self, chunk):
        if "(" in chunk:
            return self.make_regexp_query(chunk)
        else:
            return self.make_match_phrase_query(chunk)

    def make_impact_phrase_query_chunks(self, impact_phrase):
        impact_phrase_chunks = re.split("(\(.*?\))", impact_phrase)
        return [self.make_impact_phrase_query_chunk(chunk) for chunk in impact_phrase_chunks if chunk != '' and chunk != ' ']

    def make_impact_term_query(self, impact_term):
        if "(" in impact_term and " " in impact_term:
            terms = impact_term.replace("(","").replace(")","").split("|")
            return [{"match_phrase": {self.es_config["analyzed_field"]: term}} for term in terms]
        elif "(" in impact_term:
            return [{"regexp": {self.es_config["analyzed_field"]: impact_term}}]
        else:
            return [{"match_phrase": {self.es_config["analyzed_field"]: impact_term}}]

    def make_impact_context_query(self, impact_term, context_phrase):
        bool_terms = self.make_impact_phrase_query_chunks(impact_term)
        bool_terms += self.make_impact_phrase_query_chunks(context_phrase)
        return {"bool": {"must": bool_terms}}

    def make_impact_context_free_query(self, impact_term):
        return {"bool": {"must": self.make_impact_phrase_query_chunks(impact_term)}}

    def is_fiction(self, source):
        if "nur" not in source:
            return False
        if type(source["nur"]) == int and source["nur"] >= 200 and source["nur"] <= 399:
            return True
        return False

    def make_sample_sentences(self, impact_term, num_sentences):
        bool_query = {"query": self.make_impact_context_free_query(impact_term), "size": 10000}
        sentences = [hit["_source"][self.es_config["analyzed_field"]] for hit in self.search_and_filter(bool_query) if self.is_fiction(hit["_source"])]
        random.shuffle(sentences)
        if self.debug:
            print("sampling {n} of {s} sentences".format(n=num_sentences, s=len(sentences)))
        return sentences[:num_sentences]

    def generate_sample_sentences(self, impact_terms, term_group, num_sentences):
        self.sample_data = []
        for impact_term in impact_terms:
            sentences = self.make_sample_sentences(impact_term, num_sentences)
            self.sample_data += [[term_group, impact_term, sentence] for sentence in sentences]

    def write_sample_sentences(self, output_file, headers, delimiter="\t"):
        with open(output_file, 'wt') as fh:
            csvwriter = csv.writer(fh, delimiter=delimiter)
            csvwriter.writerow(headers)
            for sample in self.sample_data:
                csvwriter.writerow(sample)

