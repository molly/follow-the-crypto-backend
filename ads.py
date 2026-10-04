from google.cloud import bigquery
import datetime

from ad_videos import rehost_new_ad_creatives


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


def get_ads(db, video_limit=None):
    new_ads = {}
    ads_by_committee = {}
    gatc_to_fec = {}
    old_ads = {
        doc.id: doc.to_dict() for doc in db.client.collection("ads").stream()
    }
    for c_id, committee in db.committees.items():
        # A committee can run ads under more than one advertiser account, so
        # GATC_id accepts either a single id or a list of them.
        gatc_ids = committee.get("GATC_id") or []
        if isinstance(gatc_ids, str):
            gatc_ids = [gatc_ids]
        if not gatc_ids:
            continue
        for gatc_id in gatc_ids:
            gatc_to_fec[gatc_id] = c_id
        ads_by_committee[c_id] = {"GATC_ids": gatc_ids, "ads": {}}

    client = bigquery.Client()
    QUERY = (
        "SELECT {fields} FROM `bigquery-public-data.google_political_ads.creative_stats` "
        "WHERE advertiser_id IN ({advertisers}) "
        "AND date_range_start >= '2025-01-01' "
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
        # Carry forward the rehosted video (and any failed-attempt count) from
        # the last run. The BigQuery row doesn't know about it, so without this
        # every run would re-download every ad.
        previous = old_ads.get(gatc_to_fec[gatc_id], {}).get("ads", {}).get(ad_id, {})
        for key in ("video", "video_attempts", "image", "image_attempts"):
            if key in previous:
                ad_details[key] = previous[key]
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

    # Archive our own copy of the creative before writing, so the stored ad
    # record points at media we control rather than a link that can rot.
    rehost_new_ad_creatives(ads_by_committee, limit=video_limit)

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

    # One document per committee rather than a single "by_committee" doc:
    # the combined ad set is well past Firestore's 1MB document limit.
    for c_id, ad_group in ads_by_committee.items():
        db.client.collection("ads").document(c_id).set(ad_group)
    return new_ads
