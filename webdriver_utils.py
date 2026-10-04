"""Shared Selenium driver setup.

Used by `races.py` for scraping election results and by `ad_videos.py` for
pulling ad video out of the Ad Transparency Center, which refuses non-browser
clients regardless of cookies or headers.
"""

from selenium import webdriver

FIREFOX_PROFILE = (
    "/Users/molly/Library/Application Support/Firefox/Profiles/9e81e71e.w3igg-archiver"
)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:149.0) Gecko/20100101 Firefox/149.0"
)


def get_driver():
    options = webdriver.FirefoxOptions()
    options.profile = webdriver.FirefoxProfile(FIREFOX_PROFILE)
    options.set_preference("general.useragent.override", USER_AGENT)
    driver = webdriver.Firefox(options=options)
    return driver
