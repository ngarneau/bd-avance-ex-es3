import json
import elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.query import Match, MultiMatch

es = elasticsearch.Elasticsearch()
INDEX = "bd_avance"
DOC_TYPE = "emails"


def get_data(fname):
    with open(fname) as fhandle:
        for line in fhandle:
            data = json.loads(line, encoding='utf-8')
            data["id"] = data["_id"]["$oid"]  # Remapping the id for ES
            data["year"] = data["date"][:4]
            data["message_length"] = len(data["text"])
            del data['_id']
            yield data


def index_emails(emails):
    for e in emails:
        es.index(
            INDEX,
            DOC_TYPE,
            body=e,
            id=e['id']
        )


# 1.1
def insane_sender():
    pass


# 1.2
def inactive_users():
    pass


# 1.3
def date_histogram():
    pass


# 2.1
def distinct_senders():
    pass


# 2.2
def distinct_senders_in_2000():
    pass


# 2.3
def distinct_receiver():
    pass


# 2.4
def message_percentiles():
    pass


# 3.1
def significant_terms():
    pass


if __name__ == '__main__':
    pass
