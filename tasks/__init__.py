# Import all task modules to trigger task registration
# This must be imported in the main pipeline script

from . import committees
from . import contributions
from . import expenditures
from . import disbursements
from . import individuals
from . import individuals_selective  # New selective individual tasks
from . import company_dependencies  # Company dependency handling
from . import companies
from . import races
from . import candidates
from . import pacs
from . import ads_task
from . import recipients

__all__ = [
    "committees",
    "contributions",
    "expenditures",
    "disbursements",
    "individuals",
    "individuals_selective",
    "company_dependencies",
    "companies",
    "races",
    "candidates",
    "pacs",
    "ads_task",
    "recipients",
]
