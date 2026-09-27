import re
from src.utils import norm_text
from src.filters.india import india_score
from src.filters.remote import is_remote_job
from src.filters.support import relevance


def direct_support_job(title, description="", company=""):
    """Title-first support classifier for structured ATS jobs.

    Direct ATS data is cleaner than aggregator snippets, so require a real
    support/customer-success signal in the title. This prevents generic
    engineering/sales roles from passing merely because the description
    mentions customers or support.
    """
    title_t = norm_text(title)
    score, hits = relevance(title, description, company)
    strong = any(x in title_t for x in (
        "customer support", "customer service", "customer success", "customer experience",
        "customer care", "technical support", "product support", "application support",
        "software support", "saas support", "it support", "help desk", "helpdesk",
        "service desk", "support specialist", "support associate", "support engineer",
        "support analyst", "support manager", "support lead", "support agent", "support advisor",
        "support representative", "support coordinator", "support technician", "support consultant",
        "client support", "client success", "member support", "merchant support", "seller support",
        "partner support", "community support", "trust and safety", "content moderation",
        "implementation specialist", "implementation manager", "implementation consultant",
        "onboarding specialist", "onboarding manager", "customer onboarding", "customer enablement",
        "customer adoption", "customer retention", "account manager", "account management",
    ))
    if any(x in title_t for x in ("sales", "software engineer", "developer", "data scientist", "designer", "recruiter")) and not strong:
        return False, score, hits
    return strong and score >= 5, score, hits


def direct_india_remote(job):
    title = job.get("title", "")
    loc = job.get("location", "")
    desc = job.get("description", "")
    cc = job.get("country_code", "")
    remote = job.get("remote", False)
    work_model = job.get("work_model", "")
    if not is_remote_job(title, loc, desc, work_model, remote, cc):
        return False
    # Structured country code is strongest. Otherwise require explicit India/city evidence.
    return india_score(" ".join([title, loc, desc]), cc) >= 5
