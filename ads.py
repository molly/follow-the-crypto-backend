from google.cloud import bigquery
import datetime


def quote_str(string):
    return "'{}'".format(string)


FIELDS = [
    "advertiser_id",
    "ad_id",
    "ad_url",
    "ad_type",
    "date_range_start",
    "date_range_end",
    "impressions",
    "spend_usd",
    "age_targeting",
    "gender_targeting",
    "geo_targeting_excluded",
    "geo_targeting_included",
    "spend_range_max_usd",
    "spend_range_min_usd",
]


def get_ads(db):
    ads_by_committee = {}
    gatc_to_fec = {}
    for c_id, committee in db.committees.items():
        if "GATC_id" in committee and committee["GATC_id"]:
            gatc_to_fec[committee["GATC_id"]] = c_id
            ads_by_committee[c_id] = {"GATC_id": committee["GATC_id"], "ads": {}}

    client = bigquery.Client()
    QUERY = (
        "SELECT {fields} FROM `bigquery-public-data.google_political_ads.creative_stats` "
        "WHERE advertiser_id IN ({advertisers}) "
        "GROUP BY {fields}"
    )
    HYDRATED_QUERY = QUERY.format(
        advertisers=",".join(map(quote_str, gatc_to_fec.keys())),
        fields=", ".join(FIELDS),
    )
    query_job = client.query(HYDRATED_QUERY)
    rows = query_job.result()
    for row in rows:
        gatc_id = row["advertiser_id"]
        ad_id = row["ad_id"]
        ad_details = {}
        for field in FIELDS:
            value = row[field]
            if isinstance(value, datetime.date):
                value = value.isoformat()
            ad_details[field] = value
        ads_by_committee[gatc_to_fec[gatc_id]]["ads"][ad_id] = ad_details
    db.client.collection("ads").document("google").set(ads_by_committee)
