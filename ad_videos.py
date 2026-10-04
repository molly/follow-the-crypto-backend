"""Rehost the creative for Google political ads.

Ad creatives can't be linked to directly and can't be relied on to stick around:

  - The Ad Transparency Center only exposes video through signed
    `googlevideo.com` URLs that expire within hours, so there's nothing stable
    to embed.
  - Not every ad has a public YouTube counterpart to fall back on, and the ones
    that do can be taken down later, leaving a dead embed on the race page.

So the pipeline pulls its own copy of every new ad and rehosts it.

Extraction drives a real browser, because the Ad Transparency Center refuses
anything that isn't one. Requests from `requests` and from `curl` both get
redirected to Google's abuse interstitial at `google.com/sorry/` while Chrome
loads the same page fine from the same IP with the same cookies -- so this isn't
something headers or a cookie jar can talk their way past, and trying only earns
a penalty that outlasts an hour. Selenium was already a dependency here (see
`webdriver_utils.get_driver`), so the browser comes cheap.

Video ads play in an iframe whose `<video>` element points at a signed
`googlevideo.com` URL that expires within hours, so extraction and download
happen in one pass. Text and image ads have no video at all -- Google doesn't
expose their headline/body copy as data anywhere (not in this page, not in the
BigQuery political ads dataset), only as a flattened preview image rendered
straight into the page at `<html-renderer><div><img>`. That src isn't signed
and doesn't expire, so it's downloaded with a plain request rather than through
the browser.
"""

import json
import os
import subprocess
import tempfile
import threading
import time

import backoff
import requests
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By

from media_storage import MediaStorage
from webdriver_utils import get_driver

# Space page loads out. A browser doesn't draw the abuse interstitial the way
# a scripted client does, but a few hundred creatives in a row is still not what
# ordinary browsing looks like.
_MIN_INTERVAL_SECONDS = float(os.environ.get("ATC_MIN_INTERVAL_SECONDS", "3"))
_throttle_lock = threading.Lock()
_last_request_time = 0.0

# How long to wait for the creative's iframe and video element to render.
PAGE_TIMEOUT_SECONDS = 30

# Give up on an ad after this many failed runs so a creative that can no longer
# be resolved doesn't get retried forever on every pipeline run.
MAX_ATTEMPTS = 3

KEY_PREFIX = "ads/google"


def _throttle():
    """Block until enough time has passed since the last ATC page load."""
    global _last_request_time
    with _throttle_lock:
        wait = _MIN_INTERVAL_SECONDS - (time.monotonic() - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()


class ATCBlocked(Exception):
    """Google served its abuse interstitial instead of the page.

    Raised so the caller can stop the run: continuing to ask only deepens the
    penalty, and the queue is picked up again next run.
    """


def _check_not_blocked(driver):
    if "/sorry/" in driver.current_url:
        raise ATCBlocked(
            "Ad Transparency Center served its abuse interstitial. Stop running "
            "fetch_ads and wait it out (an hour or more); raise "
            "ATC_MIN_INTERVAL_SECONDS if it recurs."
        )


def _find_video_src(driver):
    """Scan the page's frames for a video element with a signed source.

    The ad renders in a googlesyndication safeframe that holds several `<video>`
    elements, only one of which carries a real `src` -- the others are empty
    placeholders belonging to the player -- so check them all rather than
    assuming the first.
    """
    for index in range(len(driver.find_elements(By.TAG_NAME, "iframe"))):
        driver.switch_to.default_content()
        # Re-locate frames each pass: switching frames invalidates handles
        # taken before the switch.
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        if index >= len(frames):
            break
        try:
            driver.switch_to.frame(frames[index])
            for video in driver.find_elements(By.TAG_NAME, "video"):
                source = video.get_attribute("src") or ""
                if "googlevideo.com" in source:
                    return source
        except WebDriverException:
            continue

    driver.switch_to.default_content()
    return None


def extract_video_url(driver, ad_url):
    """Return the signed googlevideo.com mp4 URL for an ad, or None.

    The player initializes well after the page reports loaded -- the `src` only
    appears once it starts buffering, around ten seconds in -- so poll rather
    than waiting on any single element.
    """
    _throttle()
    driver.get(ad_url)
    _check_not_blocked(driver)

    deadline = time.monotonic() + PAGE_TIMEOUT_SECONDS
    while True:
        source = _find_video_src(driver)
        if source:
            return source
        if time.monotonic() >= deadline:
            return None
        time.sleep(1)


def _find_image_src(driver):
    """Return the creative preview image's src, or None.

    Text and image ads render natively in the page -- no safeframe, no signed
    URL -- as a flattened preview: `<html-renderer><div><img src="...">`.
    That src points at a stable googlesyndication archive path rather than an
    expiring one, but it's still rehosted like video so the page doesn't
    depend on Google's domain staying up.
    """
    try:
        img = driver.find_element(By.CSS_SELECTOR, "html-renderer img")
    except NoSuchElementException:
        return None
    return img.get_attribute("src") or None


def extract_image_src(driver, ad_url):
    """Return the creative preview image's src for a text/image ad, or None."""
    _throttle()
    driver.get(ad_url)
    _check_not_blocked(driver)

    deadline = time.monotonic() + PAGE_TIMEOUT_SECONDS
    while True:
        source = _find_image_src(driver)
        if source:
            return source
        if time.monotonic() >= deadline:
            return None
        time.sleep(1)


def _signed_url_expired(e):
    """A 4xx from googlevideo means the signature is dead, not that it's busy."""
    response = getattr(e, "response", None)
    return response is not None and 400 <= response.status_code < 500


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=4,
    giveup=_signed_url_expired,
)
def _download(url, destination):
    """Fetch the signed mp4. Worth retrying in place rather than deferring to
    the next run, since the URL stops working after a few hours."""
    with requests.get(url, stream=True, timeout=180) as response:
        response.raise_for_status()
        with open(destination, "wb") as handle:
            for block in response.iter_content(chunk_size=1 << 16):
                handle.write(block)


def _probe(path):
    """Return (duration_seconds, width, height) for a video file."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-show_entries", "format=duration",
            "-of", "json",
            path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    stream = (data.get("streams") or [{}])[0]
    duration = data.get("format", {}).get("duration")
    return (
        round(float(duration), 2) if duration else None,
        stream.get("width"),
        stream.get("height"),
    )


def _transcode(source, destination):
    """Re-encode for the web: capped at 720px, faststart, browser-safe H.264.

    Google serves itag 22 (720p), which already plays in a browser, but it's
    several times larger than it needs to be for a 15-second clip in a small
    player. Shrinking it mostly buys faster starts on slow connections.
    """
    subprocess.run(
        [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", source,
            # Cap the long edge at 720 without upscaling, then round to even
            # dimensions, which H.264's 4:2:0 chroma subsampling requires.
            "-vf", (
                "scale='min(720,iw)':'min(720,ih)':force_original_aspect_ratio=decrease,"
                "scale=trunc(iw/2)*2:trunc(ih/2)*2"
            ),
            "-c:v", "libx264", "-crf", "28", "-preset", "slow",
            "-profile:v", "high", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "96k",
            # Put the moov atom first so playback can start before the whole
            # file has downloaded.
            "-movflags", "+faststart",
            destination,
        ],
        check=True,
        capture_output=True,
    )


def _make_poster(source, destination):
    """Grab a representative still for the <video> poster.

    `thumbnail` picks the most representative frame out of a window rather than
    taking frame one, which on these ads is usually a fade-in from black.
    """
    subprocess.run(
        [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", source,
            "-vf", "thumbnail=n=100,scale='min(720,iw)':-2",
            "-frames:v", "1", "-q:v", "4",
            destination,
        ],
        check=True,
        capture_output=True,
    )


def rehost_ad_video(driver, ad_id, ad_url, storage=None):
    """Download, transcode and upload one ad's video.

    Returns the record to store on the ad -- object keys, not URLs, so the
    frontend controls what origin they're served from -- or None if no video
    could be resolved. Raises on unexpected failures so the caller can record
    an attempt.
    """
    storage = storage or MediaStorage()

    video_key = f"{KEY_PREFIX}/{ad_id}.mp4"
    poster_key = f"{KEY_PREFIX}/{ad_id}.jpg"
    original_key = f"{KEY_PREFIX}/originals/{ad_id}.mp4"

    source_url = extract_video_url(driver, ad_url)
    if not source_url:
        return None

    with tempfile.TemporaryDirectory() as workdir:
        original = os.path.join(workdir, "original.mp4")
        transcoded = os.path.join(workdir, "web.mp4")
        poster = os.path.join(workdir, "poster.jpg")

        _download(source_url, original)
        _transcode(original, transcoded)
        _make_poster(original, poster)

        duration, width, height = _probe(transcoded)

        record = {
            "mp4": storage.upload(video_key, transcoded, "video/mp4"),
            "poster": storage.upload(poster_key, poster, "image/jpeg"),
            # The untouched download is the archival copy: these creatives
            # disappear from the ATC once an advertiser pulls them, and a
            # re-encode can't be undone.
            "original": storage.upload(original_key, original, "video/mp4"),
            "duration": duration,
            "width": width,
            "height": height,
        }

    return record


def rehost_ad_image(driver, ad_id, ad_url, storage=None):
    """Download and upload one text/image ad's preview image.

    Returns the record to store on the ad, or None if no preview image could
    be resolved. Raises on unexpected failures so the caller can record an
    attempt.
    """
    storage = storage or MediaStorage()

    source_url = extract_image_src(driver, ad_url)
    if not source_url:
        return None

    with requests.get(source_url, timeout=60) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "image/png")
        extension = content_type.split("/")[-1] or "png"
        content = response.content

    with tempfile.TemporaryDirectory() as workdir:
        downloaded = os.path.join(workdir, f"image.{extension}")
        with open(downloaded, "wb") as handle:
            handle.write(content)

        image_key = f"{KEY_PREFIX}/{ad_id}.{extension}"
        width, height = _probe_image(downloaded)

        record = {
            "src": storage.upload(image_key, downloaded, content_type),
            "width": width,
            "height": height,
        }

    return record


def _probe_image(path):
    """Return (width, height) for an image file."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "json",
            path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    stream = (json.loads(result.stdout).get("streams") or [{}])[0]
    return (stream.get("width"), stream.get("height"))


# Media kinds this rehosts, keyed by ad_type: the field each stores its result
# under, the field that counts failed attempts, and the function that does the
# actual fetch-and-upload for one ad.
_MEDIA_KINDS = {
    "VIDEO": ("video", "video_attempts", rehost_ad_video),
    "TEXT": ("image", "image_attempts", rehost_ad_image),
    "IMAGE": ("image", "image_attempts", rehost_ad_image),
}


def rehost_new_ad_creatives(ads_by_committee, limit=None):
    """Rehost the creative for every Google ad that doesn't already have one.

    Video ads get a `video` record; text and image ads -- which the Ad
    Transparency Center renders as a flattened preview image rather than
    structured text -- get an `image` record instead. Mutates
    `ads_by_committee` in place. Ads that fail get an incrementing
    `*_attempts` count and are dropped after MAX_ATTEMPTS so a permanently
    broken creative doesn't cost a network round trip on every run.
    """
    pending = [
        (ad_id, ad, *_MEDIA_KINDS[ad.get("ad_type")])
        for committee in ads_by_committee.values()
        for ad_id, ad in committee.get("ads", {}).items()
        if ad.get("type") == "google"
        and not ad.get("excluded")
        and ad.get("ad_type") in _MEDIA_KINDS
        and not ad.get(_MEDIA_KINDS[ad.get("ad_type")][0])
        and ad.get(_MEDIA_KINDS[ad.get("ad_type")][1], 0) < MAX_ATTEMPTS
    ]
    if not pending:
        print("No new ad creatives to rehost.")
        return 0

    # Untagged ads first. A poster is what makes an ad identifiable in
    # /admin/edit/ads, and assigning races is the human bottleneck -- ads that
    # already have one are already on the site and already recognizable. A full
    # backfill covers everything either way; this only orders a capped run.
    pending.sort(key=lambda item: 1 if item[1].get("race") else 0)

    if limit is not None and len(pending) > limit:
        print(f"Rehosting {limit} of {len(pending)} pending ad creatives this run.")
        pending = pending[:limit]
    else:
        print(f"Rehosting {len(pending)} ad creatives.")

    try:
        storage = MediaStorage()
    except Exception as error:
        # A storage problem shouldn't take down the whole ads task; the rest of
        # the ad data is still worth writing, and the creatives retry next run.
        print(f"Skipping ad creative rehosting: {error}")
        return 0

    rehosted = 0
    driver = get_driver()
    try:
        for ad_id, ad, media_field, attempts_field, rehost in pending:
            try:
                record = rehost(driver, ad_id, ad["ad_url"], storage=storage)
            except ATCBlocked as error:
                # Not this ad's fault, so don't charge it an attempt -- and stop
                # asking, because every further request extends the block. The
                # rest of the queue keeps until the next run.
                print(f"Stopping ad creative rehosting after {rehosted}: {error}")
                break
            except Exception as error:
                print(f"Failed to rehost {ad_id}: {error}")
                ad[attempts_field] = ad.get(attempts_field, 0) + 1
                continue

            if record is None:
                print(f"No creative found for {ad_id}")
                ad[attempts_field] = ad.get(attempts_field, 0) + 1
                continue

            ad[media_field] = record
            ad.pop(attempts_field, None)
            rehosted += 1
    finally:
        driver.quit()

    print(f"Rehosted {rehosted} ad creatives.")
    return rehosted
