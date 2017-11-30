from impact_model import impact_groups
from query_sentence_sampling import QuerySentenceSampler
import csv

def read_test_reviewids(fname):
    with open(fname, 'rt') as fh:
        csvreader = csv.reader(fh, delimiter="\t")
        next(csvreader)
        return ["\t".join(row) for row in csvreader]

test_reviewids_file = "odbr-test-reviewids.tsv"
test_reviewids = read_test_reviewids(test_reviewids_file)
es_config = {
	"port": 9200,
	"index": "odbr-reviews",
	"doc_type": "sentence",
	"analyzed_field": "sentence",
	"regexp_field": "sentence_not_analyzed",
}
query_sentence_sampler = QuerySentenceSampler(es_config, ignore_ids=test_reviewids)

headers= ["term_group", "impact_term", "sentence", "expresses_impact", "expresses_lack_of_impact", "expresses_impact_emotion"]
# Selecting a random sample of sentences independent of the presence of impact terms
num_sentences = 200
output_file = "Impact_sentences/impact-sentences-no-impact-term.tsv"
query_sentence_sampler.generate_sample_sentences([""], "no-impact-term", num_sentences)
query_sentence_sampler.write_sample_sentences(output_file, headers)


num_sentences = 20
for impact_group in impact_groups:
    output_file = "Impact_sentences/impact-sentences-{g}.tsv".format(g=impact_group)
    print("Sampling sentences for terms in impact group {g}".format(g=impact_group))
    query_sentence_sampler.generate_sample_sentences(impact_groups[impact_group]["terms"], impact_group, num_sentences)
    query_sentence_sampler.write_sample_sentences(output_file, headers)



