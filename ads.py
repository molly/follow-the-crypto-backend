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
    new_ads = {}
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
        if ad_id in db.ads["google"]:
            ad_details.update(db.ads["google"][ad_id])
        ad_details["fec_id"] = gatc_to_fec[gatc_id]
        ads_by_committee[gatc_to_fec[gatc_id]]["ads"][ad_id] = ad_details
        ads_by_committee[gatc_to_fec[gatc_id]]["ads"][ad_id]["type"] = "google"

    image_ads = db.ads["images"]
    for ind, ad in enumerate(image_ads):
        image_ads[ind]["type"] = "image"
        if ad["committee_id"] in ads_by_committee:
            ads_by_committee[ad["committee_id"]]["ads"][ad["src"]] = ad
        else:
            ads_by_committee[ad["committee_id"]] = {"ads": {ad["ad_id"]: ad}}

    old_ads = db.client.collection("ads").document("by_committee").get().to_dict()
    if old_ads:
        for committee in ads_by_committee:
            if committee not in old_ads:
                new_ads[committee] = ads_by_committee[committee]["ads"]
            else:
                old_ad_ids = set(old_ads[committee]["ads"].keys())
                for ad_id in ads_by_committee[committee]["ads"]:
                    if ad_id not in old_ad_ids:
                        if committee not in new_ads:
                            new_ads[committee] = {}
                        new_ads[committee][ad_id] = ads_by_committee[committee]["ads"][
                            ad_id
                        ]

    db.client.collection("ads").document("by_committee").set(ads_by_committee)
    return new_ads
