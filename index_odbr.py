import nltk
import json
import csv
import re
import unicodedata
import treetaggerwrapper

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from dateutil.parser import parse
from elasticsearch import helpers

es = Elasticsearch()

"""
The book reviews from the ODBR corpus are indexed in Elasticsearch.

Preprocessing steps before indexing:
- Review text is normalized, special characters are transformed or removed and HTML comments are removed.
- Review text is sentence tokenized and individual sentences are POS tagged using TreeTagger (and the Python wrapper TreeTaggerWrapper, https://perso.limsi.fr/pointal/doku.php?id=dev:treetaggerwrapper).
- Original and POS-tagged sentences are added to the review document.
- Reviews for which the reviewdate field has no date or a malformed date string are indexed without a reviewdate field.

The index name is odbr-review and the document type is review.
"""


def shave_marks(txt):
    """This method removes all diacritic marks from the given string"""
    norm_txt = unicodedata.normalize('NFD', txt)
    shaved = ''.join(c for c in norm_txt if not unicodedata.combining(c))
    return unicodedata.normalize('NFC', shaved)

def remove_html_comments(text):
    if "<!--" in text and "-->" in text:
        text = re.sub("<!--.*?-->", "", text, flags=re.DOTALL)
    if "<![CDATA[" in text:
        text = re.sub("<!\[CDATA\[.*?\]\]\&gt;", "", text, flags=re.DOTALL)
    if "\t\t" in text:
        text = re.sub("\t+", "\t", text, flags=re.DOTALL)
    if "\n\n" in text:
        text = re.sub("\n+", "\n", text, flags=re.DOTALL)
    return text

def insert_missing_whitespace(text):
    m = re.search(" \w+[\.\?:][A-Z]\w+", text)
    while m:
        text = re.sub(r"( \w+[\.\?:])([A-Z]\w+)", r"\1 \2", text)
        m = re.search(" \w+[\.\?:][A-Z]\w+", text)
    return text

def clean_text(text):
    text = shave_marks(remove_html_comments(text))
    # replace fancy quotes
    text = re.sub("[‚`´’‘]", "'", text)
    text = re.sub('[«»„‟“”]', '"', text)
    # replace special double characters
    text = re.sub('ß', 'ss', text)
    text = re.sub('æ', 'ae', text)
    text = re.sub('ﬁ', 'fi', text)
    text = re.sub('ﬂ', 'fl', text)
    # remove frequent special characters
    text = re.sub("[\x96\x80\x97\x91\x92\x93\x94\xad\u200e\u202c\u202a]", " ", text)
    text = re.sub("[²±×★¼½|•€©°・·…]", " ", text)
    # replace fancy dashes
    text = re.sub("[–—−‑]", "-", text)
    # remove uninterpreted punctuation
    text = re.sub("[~\[\]\r\t\n@#%\{\}_]", " ", text)
    # collapse whitespace
    text = re.sub("\s+", " ", text)
    # strip leading and trailing whitespace
    text = text.strip()
    text = insert_missing_whitespace(text)
    return text

def make_metadata_json(row, headers):
    review_json = {}
    for index, header in enumerate(headers):
        review_json[header] = row[index]
    return review_json


def csv_to_json(fname, delimiter):
    with open(fname, 'rt') as fh:
        csvreader = csv.reader(fh, delimiter=delimiter)
        headers = next(csvreader)
        headers = [header.lower().replace(" ", "_") for header in headers]
        for row in csvreader:
            yield make_metadata_json(row, headers)

def index_review(doc, doc_type="review"):
    es.index(index="odbr-reviews", doc_type=doc_type, id=doc["doc_id"], body=doc)

def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False

def pos_tag_sentences(sentences):
    global tagger
    pos_tagged_sentences = []
    for sentence in sentences:
        try:
            pos_tagged_sentences += [tagger.tag_text(sentence, notagurl=True, notagemail=True, notagip=True, notagdns=True)]
        except treetaggerwrapper.TreeTaggerError:
            print(sentence)
            tagger = treetaggerwrapper.TreeTagger(TAGLANG='nl')
    return pos_tagged_sentences

def process_review(review):
    check_dates(review)
    review["text"] = clean_text(review["text"])
    integer_fields = ["year", "birthyear", "upvotes", "downvotes", "rating", "nur"]
    for integer_field in integer_fields:
        try:
            review[integer_field] = int(review[integer_field])
        except ValueError:
            pass

def check_dates(review):
    if not is_date(review["reviewdate"]):
        del review["reviewdate"]

def create_transaction(source, doc_type, index="odbr-reviews"):
    return {
        "_index": index,
        "_type": doc_type,
        "_id": source["sentence_id"],
        "_source": source
    }

def scroll_collection(index="odbr_reviews", doc_type="review"):
    response = es.search(index=index, doc_type=doc_type, body={"size": 10000}, scroll = '2m')
    for hit in response['hits']['hits']:
        yield hit
    while len(response['hits']['hits']) > 0:
        response = es.scroll(scroll_id = response["_scroll_id"], scroll = '2m')
        for hit in response['hits']['hits']:
            yield hit

def index_reviews():
    for index, review in enumerate(csv_to_json(reviews_file, "\t")):
        process_review(review)
        try:
            review["sentences"] = [sent for sent in nltk.sent_tokenize(review["text"])]
            review["tagged_sentences"] = pos_tag_sentences(review["sentences"])
            review["doc_id"] = index+1
            index_review(review)
        except TransportError:
            print(json.dumps(review, indent=4))
            raise
        if (index+1) % 1000 == 0:
            print("{0} reviews indexed".format(index+1))

def index_sentences():
    doc_type = "sentence"
    sent_sum = 0
    transactions = []
    for doc_index, hit in enumerate(scroll_collection(index="odbr-reviews", doc_type="review")):
        for sent_index, sent in enumerate(hit["_source"]["tagged_sentences"]):
            doc_id = "{d}-{s}".format(d=hit["_id"], s = sent_index)
            doc = {
                "sentence_id": doc_id,
                "review_id": hit["_id"],
                "sentence_index": sent_index,
                "sentence": hit["_source"]["sentences"][sent_index],
                "sentence_not_analyzed": hit["_source"]["sentences"][sent_index],
                "tagged_sentence": sent,
                "nur": hit["_source"]["nur"]
            }
            transactions += [create_transaction(doc, doc_type)]
            sent_sum += 1
        if (doc_index+1) % 10000 == 0:
            print("{d} docs and {s} sentences indexed".format(d=doc_index+1, s=sent_sum))
            helpers.bulk(es, transactions)
            transactions = []

tagger = treetaggerwrapper.TreeTagger(TAGLANG='nl')
reviews_file = "allreviews.csv"

index_reviews()

index_sentences()
