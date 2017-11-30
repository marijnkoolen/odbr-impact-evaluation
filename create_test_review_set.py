from elasticsearch import Elasticsearch
import csv, re
import random

es = Elasticsearch()

def check_text_overlap(text1, text2):
    terms1 = re.findall("\w+", text1)
    terms2 = re.findall("\w+", text2)

    overlap = 0
    for term in terms1:
        if term in terms2:
            del terms2[terms2.index(term)]
            overlap += 1
    print(len(terms1), overlap, len(terms2), overlap / (len(terms1) + len(terms2)))
    if overlap / (len(terms1) + len(terms2)) < 0.5:
        print(text1)
        print()
        print(text2)
    return overlap / (len(terms1) + len(terms2))

def handle_review(review, reviews):
    identifier = "{a}\t{b}\t{c}".format(a=review["accountid"], b=review["bookid"], c=review["collectionname"])
    if identifier in identifier_map:
        if review["text"] == reviews[identifier]["text"]:
            continue
        elif review["reviewdate"] != review["reviewdate"]:
            print("different date")
        else:
            if len(review["text"]) > len(reviews[identifier]["text"]):
                reviews[identifier] = review
    reviews[identifier] = review
    return identifier

def make_test_set(identifiers, identifier_map):
    random.shuffle(identifiers)
    return identifiers[:1000]

def test_mapping(test_identifiers, test_reviewids):
    # validate reviewid mapping
    for reviewid in sorted(test_reviewids):
        response = es.get(index="odbr-reviews", doc_type="review", id=reviewid)
        review = response["_source"]
        identifier = "{a}\t{b}\t{c}".format(a=review["accountid"], b=review["bookid"], c=review["collectionname"])
        if identifier not in test_identifiers:
            print("Error!")
            print(reviewid, identifier)

def write_test_identifiers(test_identifiers, test_identifiers_file):
    headers = "accountid\tbookid\tcollectionname"
    with open(test_identifiers_file, 'wt') as fh:
        fh.write("{line}\n".format(line=headers))
        for identifier in test_identifiers:
            fh.write("{line}\n".format(line=identifier))

def read_test_identifiers(fname):
    with open(fname, 'rt') as fh:
        csvreader = csv.reader(fh, delimiter="\t")
        next(csvreader) # header row
        return ["\t".join(row) for row in csvreader]

def write_test_reviewids(test_reviewids, test_reviewids_file):
    headers = "reviewid"
    with open(test_reviewids_file, 'wt') as fh:
        fh.write("{line}\n".format(line=headers))
        for reviewid in test_reviewids:
            fh.write("{line}\n".format(line=reviewid))

def read_test_reviewids(fname):
    with open(fname, 'rt') as fh:
        csvreader = csv.reader(fh, delimiter="\t")
        next(csvreader) # header row
        return ["\t".join(row) for row in csvreader]

def make_metadata_json(row, headers):
    review_json = {}
    for index, header in enumerate(headers):
        review_json[header] = row[index]
    return review_json


def read_csv_to_json(fname, delimiter):
    with open(fname, 'rt') as fh:
        csvreader = csv.reader(fh, delimiter=delimiter)
        headers = next(csvreader)
        headers = [header.lower().replace(" ", "_") for header in headers]
        for row in csvreader:
            yield make_metadata_json(row, headers)

#test_identifiers = make_test_set(identifiers, identifier_map)


review_file = "allreviews.csv"
identifier_map = {}
identifiers = []
reviews = {}

for index, review in enumerate(read_csv_to_json(review_file, "\t")):
    if review["accountid"] == "": user = review["name"]
    identifier = handle_review(review, reviews)
    identifier_map[identifier] = index+1
    identifiers += [identifier]
    if (index+1) % 10000 == 0:
        print("{n} reviews processed".format(n=index+1))

print("{n} reviews processed".format(n=index+1))


#test_reviewids = [identifier_map[identifier] for identifier in test_identifiers]
test_identifiers_file = "odbr-test-identifiers.tsv"
test_reviewids_file = "odbr-test-reviewids.tsv"
#write_test_reviewids(test_reviewids, test_reviewids_file)
#write_test_identifiers(test_identifiers, test_identifiers_file)

