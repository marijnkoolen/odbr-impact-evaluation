import requests
from collections import defaultdict, Counter
import os
import csv
import re
from impact_model import impact_contexts

def make_metadata_json(row, headers):
    review_json = {}
    for index, header in enumerate(headers):
        review_json[header] = row[index]
    return review_json

spreadsheet_export_urls = {
    "verb_transitive_subject_object": "https://docs.google.com/spreadsheets/d/1m0GvwEEvJ7ONmEbiVPMy28DomB3qkWKEl5rIeZZGhH8/export?format=tsv&gid=409235815",
    "verb_phrase": "https://docs.google.com/spreadsheets/d/1PPNukHcDKO-giALheOWW-UIMeMkMZZx_ksyht-VnRdI/export?format=tsv&gid=178216830",
    "verb_object_pronoun": "https://docs.google.com/spreadsheets/d/1W_g2CLfNKvk-arFFbmtDQS40wf4UWp2xpOuZJR1gHVo/export?format=tsv&gid=669385086",
    "verb_infinitive_auxiliary": "https://docs.google.com/spreadsheets/d/1N3SIf9GGzh7XH6u9bsWNZSj2HsIIl8uSvDeffccAsiU/export?format=tsv&gid=1327648403",
    "verb_gerund_transitive_object_pronoun": "https://docs.google.com/spreadsheets/d/11qisLOmsd0enzccKeUdOOv6Wrr8Z4ZgRLi0gam_0b3o/export?format=tsv&gid=20351626",
    "verb_gerund_subject_pronoun": "https://docs.google.com/spreadsheets/d/1Ja2THWhiFQBIp5WCrRUKY0WztVptW7zNn28ETlTdBjk/export?format=tsv&gid=1082413371",
    "pronoun_get_noun": "https://docs.google.com/spreadsheets/d/1_hmaV4xOA1X2WRXU6h4EHYiKQQeQt7UDsbOo0uiD3T0/export?format=tsv&gid=1305734426",
    "noun_referent_be": "https://docs.google.com/spreadsheets/d/1j3kh6coVhk90Du5Tyg5E04I1lZBukfb5RW9FyG8hOak/export?format=tsv&gid=2130316221",
    "noun_phrase": "https://docs.google.com/spreadsheets/d/1M1MTyIDAmeygbhAq-ZBeg4rxmaNsLzIl54M5ydeWAhk/export?format=tsv&gid=1216248770",
    "noun_character_be": "https://docs.google.com/spreadsheets/d/1wwKyY1iYQQZhnfqq9bVDYToGdt4nIiNuPjA7WOWWNOs/export?format=tsv&gid=1398798681",
    "adjective_get": "https://docs.google.com/spreadsheets/d/1nVyTbX7Kt2pv6FXxJIUy8-8q3cJcTnmyNrKOhEYvvLs/export?format=tsv&gid=853947906",
    "adjective_find": "https://docs.google.com/spreadsheets/d/1kzWUdFEg6_RJB2-OxeEoMmveeGvuLcBPYUS_2yLQU-M/export?format=tsv&gid=1188563828",
    "adjective_be": "https://docs.google.com/spreadsheets/d/1nOtB9j41VZQb3F9Kc1a0JiJbFgUu_RZIPw4zSRqAm0s/export?format=tsv&gid=1850769201",
    "additional_verb": "https://docs.google.com/spreadsheets/d/1KhDU77fR4MW41w_nFPgNjsNbwg8mcEik0Cb8O0b5A3M/export?format=tsv&gid=942479183",
    "additional_verb_phrase": "https://docs.google.com/spreadsheets/d/1QD2ugalr-oU5SkezTVH7dtfifkxFlcqTI5aXnsCRzls/export?format=tsv&gid=553063338",
    "additional_noun": "https://docs.google.com/spreadsheets/d/1RXqf3Q1VSy4rqfL7CuHFJjzwE1GkyxJnjjLzYHWKwTQ/export?format=tsv&gid=1347143804",
    "additional_noun_phrase": "https://docs.google.com/spreadsheets/d/1ui52-ODaEAbW_SqK9UcCMFpShdzaStStya_rJ_r7Yqw/export?format=tsv&gid=658937911",
    "additional_adjectives": "https://docs.google.com/spreadsheets/d/16wVYg88WobUcA9mFiJokdj4k-mCJpZIp9MuekmA4lCc/export?format=tsv&gid=528357800",
}

spreadsheet_export_url_overall = "https://docs.google.com/spreadsheets/d/1vHHXy2Hj8Dhbzrg-P0m1YWEZnVYiPryprM7yf271UhY/export?format=tsv&gid=1660207889"

def get_labeled_data(impact_group, spreadsheet_export_url):
    output_file = "Labeled_sentences/labeled_sentences-{i}.tsv".format(i=impact_group)
    if not os.path.isfile(output_file):
        fetch_labeled_data(output_file, spreadsheet_export_url)
    return read_labeled_data(output_file)

def fetch_labeled_data(output_file, spreadsheet_export_url):
    response = requests.get(spreadsheet_export_url)
    with open(output_file, 'wt') as fh:
        fh.write(response.text)

def read_labeled_data(output_file):
    with open(output_file, 'rt') as fh:
        lines = [line.rstrip("\n") for line in fh]
        headers = lines[0].split("\t")[:6]
        return [make_metadata_json(line.split("\t")[:6], headers) for line in lines[1:]]

data = {}
for impact_group in spreadsheet_export_urls:
    print("parsing group", impact_group)
    data[impact_group] = get_labeled_data(impact_group, spreadsheet_export_urls[impact_group])

# cluster groupings
transformed = {
    # noun terms
    "noun_term_book_related": data["noun_referent_be"] + data["noun_character_be"] +  data["additional_noun"],
    "noun_term_reader_related": data["pronoun_get_noun"],
    # phrases
    "noun_phrase": data["noun_phrase"] + data["additional_noun_phrase"],
    "verb_phrase": data["verb_phrase"] + data["additional_verb_phrase"],
    # verb terms
    "verb_term": data["verb_transitive_subject_object"] +
        data["verb_object_pronoun"] +
        data["verb_infinitive_auxiliary"] +
        data["verb_gerund_transitive_object_pronoun"] +
        data["verb_gerund_subject_pronoun"] +
        data["additional_verb"],
    "adjective_term_book_related": data["adjective_find"] + data["additional_adjectives"],
    "adjective_term_reader_related": data["adjective_get"] + data["adjective_be"],
}



impact_term_set = set()
impact_group = defaultdict(set)

for term_group in transformed:
    for row in transformed[term_group]:
        impact_term_set.add(row["impact_term"])
        impact_group[row["impact_term"]].add(term_group)

print(len(impact_term_set))
freq = Counter()
match = 0
group_matches = Counter()
data_overall = get_labeled_data("no_impact_term", spreadsheet_export_url_overall)
output_file = "Impact_sentences/impact-sentences-no-impact-term-matched.tsv"
with open(output_file, 'wt') as fh:
    csvwriter = csv.writer(fh, delimiter="\t")
    headers = list(data_overall[0].keys())  + ["impact_matches"]
    csvwriter.writerow(headers)
    for row in data_overall:
        impact_matches = []
        match = "n"
        for term in impact_term_set:
            search_string = r"\b%s\b" % term
            term = term.replace("*", "\*")
            if re.search(r"\b%s\b" % term, row["sentence"], flags=re.IGNORECASE):
                impact_matches += [term]
                match = "j"
                group_matches.update(impact_group[term])
        row["impact_matches"] = ", ".join(impact_matches)
        #print(row)
        freq.update([(row["expresses_impact"], match)])
        csvwriter.writerow(list(row.values()))

    print(freq)

print(group_matches)

context_insensitive = []
context_sensitive = []
irrelevant = []

total_sentences = {}
total_impact = {}
total_labels = {}
freq = {}
ratio_freq = Counter()

total_sentences["total"] = defaultdict(int)
total_impact["total"] = defaultdict(int)
total_labels["total"] = defaultdict(int)
freq["total"] = {}

for term_group in transformed:
    total_sentences[term_group] = defaultdict(int)
    total_impact[term_group] = defaultdict(int)
    total_labels[term_group] = defaultdict(int)
    freq[term_group] = {}
    for row in transformed[term_group]:
        # Data quality check:
        if row["expresses_impact"] not in ["j", "n", "o"]:
            print("ERROR!", row)
        if row["impact_term"] not in freq["total"]:
            freq["total"][row["impact_term"]] = Counter()
        if row["impact_term"] not in freq[term_group]:
            freq[term_group][row["impact_term"]] = Counter()
        freq[term_group][row["impact_term"]].update([row["expresses_impact"], "total"])
        freq["total"][row["impact_term"]].update([row["expresses_impact"], "total"])
        total_sentences[term_group][row["impact_term"]] += 1
        total_sentences["total"][row["impact_term"]] += 1
        if row["expresses_impact"] == "j":
            total_impact[term_group][row["impact_term"]] += 1
            total_impact["total"][row["impact_term"]] += 1
        total_labels[term_group][row["expresses_impact"]] += 1
        total_labels[term_group]["t"] += 1
        total_labels["total"][row["expresses_impact"]] += 1
        total_labels["total"]["t"] += 1

for term in freq["total"]:
    impact_ratio = 0.0
    impact_uncertainty = 0.0
    if "j" in freq["total"][term]:
        impact_ratio = freq["total"][term]["j"] / freq["total"][term]["total"]
    if "o" in freq["total"][term]:
        impact_uncertainty = freq["total"][term]["o"] / freq["total"][term]["total"]
    if impact_ratio == 1.0:
        context_insensitive += [term]
    elif impact_ratio > 0.0:
        context_sensitive += [term]
    else:
        irrelevant += [term]
    ratio_freq.update([int(impact_ratio*10)/10])

print(len(context_insensitive))
print(len(context_sensitive))
print(len(irrelevant))
print(irrelevant)
print(ratio_freq)

# Calculate recall and precision of impact terms in the context-sensitive groups

total_precision = {}
total_recall = {}
total_precision["total"] = {}
total_recall["total"] = {}

for term_group in transformed:
    total_precision[term_group] = defaultdict(float)
    total_recall[term_group] = defaultdict(float)
    for term in freq[term_group]:
        if term in context_insensitive:
            total_precision["total"][term] = 1.0
            total_recall["total"][term] = 1.0
        elif term in irrelevant:
            total_precision["total"][term] = 0.0
            total_recall["total"][term] = "NA"
        elif term in total_impact[term_group]:
            total_precision[term_group][term] = total_impact[term_group][term] / total_sentences[term_group][term]
            total_recall[term_group][term] = total_impact[term_group][term] / total_impact[term_group][term] # Duh
            total_precision["total"][term] = total_impact["total"][term] / total_sentences["total"][term]
            total_recall["total"][term] = total_impact["total"][term] / total_impact["total"][term] # Duh
    print("term_group: {g}, terms: {t}, sentences: {s}, impact: {j}, non-impact: {n}, not sure: {o}, precision: {p}".format(g=term_group, t=len(freq[term_group].keys()), s=total_labels[term_group]["t"], j=total_labels[term_group]["j"], n=total_labels[term_group]["n"], o=total_labels[term_group]["o"], p=total_labels[term_group]["j"] / total_labels[term_group]["t"]))

term_group = "total"
print("term_group: {g}, terms: {t}, sentences: {s}, impact: {j}, non-impact: {n}, not sure: {o}, precision: {p}".format(g=term_group, t=len(total_sentences[term_group].keys()), s=total_labels[term_group]["t"], j=total_labels[term_group]["j"], n=total_labels[term_group]["n"], o=total_labels[term_group]["o"], p=total_labels[term_group]["j"] / total_labels[term_group]["t"]))


def has_one_of_contexts(text, contexts, impact_term):
    for context in contexts:
        if has_context(text, context, impact_term):
            return True
    return False

def has_context(text, context, impact_term):
    for context_chunk in context:
        if "{IMPACT_TERM}" in context_chunk:
            context_chunk = context_chunk.replace("{IMPACT_TERM}", impact_term)
        if not re.search(context_chunk, text, flags=re.IGNORECASE):
            #print(row)
            return False
    return True

def has_other_impact_terms(text, impact_term_set, impact_term):
    for term in impact_term_set:
        if term == "*zucht*":
            term = "\*zucht\*"
        try:
            if term != impact_term and has_context(text, [term], impact_term):
                return True
        except:
            print("Error with term:", term)
            raise
    return False

def filter_context(term_group, label_rows, term_group_contexts, context_sensitive, imact_term_set):
    filtered = {
        "sentences": defaultdict(int),
        "impact": defaultdict(int),
        "labels": defaultdict(int),
    }
    contexts = term_group_contexts["default"]
    if term_group in term_group_contexts:
        contexts = term_group_contexts[term_group]

    for row in label_rows:
        #if row["impact_term"] in context_sensitive and "adjective_be" in term_group:
        #    print(row["impact_term"], row["expresses_impact"] == "j", row["sentence"])
        #if row["impact_term"] not in context_sensitive:
        #    continue
        if row["impact_term"] in irrelevant:
            continue
        if row["impact_term"] in context_sensitive:
            if has_one_of_contexts(row["sentence"], contexts, row["impact_term"]):
                pass
            elif has_other_impact_terms(row["sentence"], impact_term_set, row["impact_term"]):
                pass
            else:
                if term_group == "additional_adjectives":
                #if term_group == "adjective_be":
                    print(row["impact_term"], row["expresses_impact"] == "j", row["sentence"])
                continue
        filtered["labels"][row["expresses_impact"]] += 1
        filtered["labels"]["t"] += 1
        filtered["sentences"][row["impact_term"]] += 1
        if row["expresses_impact"] == "j":
            filtered["impact"][row["impact_term"]] += 1
            #print(context_impact["impact_term"])
    return filtered

def compute_filter_impact(term_group, filtered, total_sentences, total_impact):
    context = {
        "precision": defaultdict(float),
        "recall": defaultdict(float),
        "reduction": defaultdict(float),
    }
    for term in total_sentences[term_group]:
        #print(term, context_impact[term], term in context_sensitive)
        if term in total_impact[term_group]:
            if term in filtered["sentences"] and filtered["sentences"][term] > 0.0:
                context["precision"][term] = filtered["impact"][term] / filtered["sentences"][term]
            context["recall"][term] = filtered["impact"][term] / total_impact[term_group][term]
            #print(term, total_impact[term_group][term], filtered["impact"][term], total_precision[term_group][term])
            context["reduction"][term] = 1 - (filtered["sentences"][term] / total_sentences[term_group][term])
            if term_group == "additional_adjectives" and term in total_precision[term_group]:
            #if term_group == "adjective_be" and term in total_precision[term_group]:
                print(term_group, term, total_precision[term_group][term], context["precision"][term], filtered["impact"][term], total_impact[term_group][term])
    num_terms = len(filtered["sentences"].keys())
    num_sents = sum(filtered["sentences"].values())
    num_impact = sum(filtered["impact"].values())
    print("{g}, filtered terms: {t}, sents: {s}, impact: {i}".format(g=term_group, t=num_terms, s=num_sents, i=num_impact))
    return context

context_precision = {}
context_recall = {}
context_reduction = {}
context_impact = {
    "total": 0
}
context_sentences = {
    "total": 0
}

impact_term_set = set([row["impact_term"] for term_group in transformed for row in transformed[term_group]])

for term_group in transformed:

    filtered = filter_context(term_group, transformed[term_group], impact_contexts, context_sensitive, impact_term_set)
    filtered_impact = compute_filter_impact(term_group, filtered, total_sentences, total_impact)

    context_impact[term_group] = sum(filtered["impact"].values())
    context_sentences[term_group] = sum(filtered["sentences"].values())
    context_impact["total"] += sum(filtered["impact"].values())
    context_sentences["total"] += sum(filtered["sentences"].values())
    context_precision[term_group] = context_impact[term_group] / context_sentences[term_group]
    context_recall[term_group] = context_impact[term_group] / total_labels[term_group]["j"]
    context_reduction[term_group] = 1 - (context_sentences[term_group] / total_labels[term_group]["t"])
    print("%s precision %.2f (%.2f) recall %.2f reduction %.2f" % (term_group, context_precision[term_group], total_labels[term_group]["j"] / total_labels[term_group]["t"], context_recall[term_group], context_reduction[term_group]))
    print()

term_group = "total"
context_precision[term_group] = context_impact[term_group] / context_sentences[term_group]
context_recall[term_group] = context_impact[term_group] / total_labels[term_group]["j"]
context_reduction[term_group] = 1 - (context_sentences[term_group] / total_labels[term_group]["t"])
print("%s precision %.2f (%.2f) recall %.2f reduction %.2f" % (term_group, context_precision[term_group], total_labels[term_group]["j"] / total_labels[term_group]["t"], context_recall[term_group], context_reduction[term_group]))
