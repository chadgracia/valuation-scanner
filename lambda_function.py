"""
valuation_scanner.py  — catalyst edition
─────────────────────────────────────────
Daily Lambda (EventBridge, 3am UTC = 6am Kyiv).

Searches Brave for material, price-relevant catalysts on the companies we
cover, and writes a single one-line catalyst sentence to each company's
Catalyst field in Pipeline. Emails a summary to Chad and Kate.

WHAT IT WRITES
  - Catalyst field ONLY (custom_label_3999603), one short sentence.
  - It does NOT write LR Val/Date/Series/PPS. Those stay Kate's manual job;
    the email reports the round details so she can enter them.

CATALYST CONTENT (recomputed per run, single line, priority order)
  1. A recent NON-round catalyst — IPO announced, M&A / takeover, tender
     offer/buyback, distress/shutdown, or a forward/rumored raise.
  2. Else a just-closed round that the LR column has NOT caught up to yet
     (LR Date empty or older than the round) -> "Closed $XB round Mon 'YY -
     details pending". This fills the gap while Kate's LR entry lags.
  3. Else nothing. And if a stored round-pending line's round has since been
     entered into the LR column, it is cleared (the LR column is authoritative).

SOURCE BLOCKLIST
  - Results from aggregator/profile domains (Forge, Tracxn, PitchBook, etc.)
    are dropped in code before the model ever sees them — the prompt-level
    rejection of these sources wasn't reliable. Extend SOURCE_BLOCKLIST as
    new offenders are found.

IDENTITY CHECK
  - Each company's CRM `description` (native top-level field) is passed into the
    Bedrock screen so a result about a different company that merely shares a
    name (e.g. "Scout AI" the defense lab vs "Scout Motors" the carmaker) is
    rejected. Degrades to name-only behaviour if a description is blank.

UNIVERSE (union)
  - Every company held in any client portfolio (gracia-portfolios) - top
    priority, scanned first, exempt from the tier cap.
  - PLUS the deal-driven tiers:
      Tier 1 DAILY  : any FIRM deal OR >=3 FIRM+INQUIRY deals
      Tier 2 WEEKLY : INQUIRY / HOLD / CONFIRM (Mondays)
      Tier 3 MONTHLY: Traded-Issuer org type (1st of month)
    MATCHED deals ignored.

DRY_RUN=true (default) -> searches, emails what it WOULD write, no CRM writes.
                          ALSO: scans the full universe (all tiers, uncapped)
                          and skips the seen-articles de-dup, so a dry run is a
                          complete harvest rather than being thinned by prior
                          live history. Flip to false for normal scheduled
                          operation (writes the Catalyst field, normal tiers).
"""

import json
import logging
import os
import re
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config
CACHE_BUCKET     = "full-pipeline-cache"
PORTFOLIO_BUCKET = "gracia-portfolios"
SES_SENDER       = "agent@agent.graciagroup.com"
CHAD_EMAIL       = "cgracia@rainmakersecurities.com"
KATE_EMAIL       = "kate@graciagroup.com"
BEDROCK_MODEL    = "us.anthropic.claude-haiku-4-5"
DRY_RUN          = os.environ.get("DRY_RUN", "true").lower() == "true"
BRAVE_API_KEY    = os.environ.get("BRAVE_API_KEY", "")

# Reject dated news older than this. Undated/forward items are allowed through.
MAX_ANNOUNCEMENT_AGE_DAYS = 75

# Deal stage IDs
FIRM_STAGE    = 111800
INQUIRY_STAGE = 2109142
HOLD_STAGE    = 2094373
CONFIRM_STAGE = 2388323
MATCHED_STAGE = 2381534

# Org type - Traded Issuer (formerly "Unicorn"; same id). Tier-3 long tail.
TRADED_ISSUER_ID = 5103523
ORG_TYPE_FIELD   = "custom_label_625142"

# The ONLY field this scanner writes.
CATALYST_FIELD = "custom_label_3999603"

# LR fields are READ to gate the round-close lag line - never written here.
LR_DATE_FIELD = "custom_label_3826032"

# Marker suffix on a round-close line so the self-clear pass can recognise it.
PENDING_MARKER = "- details pending"

# Source blocklist - aggregator / data-profile domains whose pages are undated,
# evergreen, and carry historical round data the model misreads as current
# (this is what produced the bad Agility "$150M raise" from a Forge profile).
# Enforced in code; the prompt-level rejection of these wasn't reliable.
# Seeded conservatively (confirmed offender + the aggregators already named in
# the screen prompt); extend as new bad domains turn up in the harvest.
# The editable blocklist lives in S3, in the pipeline-token bucket alongside the
# JWT the scanner already reads (so no new bucket or permissions). Edit that file
# to add/remove domains - no redeploy; takes effect on the next run.
BLOCKLIST_BUCKET = "pipeline-token"
BLOCKLIST_KEY    = "source-blocklist.json"

SOURCE_BLOCKLIST = {
    "forgeglobal.com", "tracxn.com", "pitchbook.com", "crunchbase.com",
    "zoominfo.com", "wikipedia.org", "premieralts.com", "accessipos.com",
}

# Active set used at runtime = the seed above UNION the editable S3 list.
# Refreshed at the start of each run; the seed is the fallback floor if the S3
# file is missing or malformed.
_BLOCKLIST = set(SOURCE_BLOCKLIST)

# Per-run caps (held bucket is exempt). Bypassed entirely in DRY_RUN.
TIER1_CAP = 60
TIER2_CAP = 40
TIER3_CAP = 30

TIMEOUT_BUFFER_MS = 45_000

ROUND_EVENTS    = {"round_closed", "round_undisclosed"}
NONROUND_EVENTS = {"round_targeting", "ipo_announced", "tender_offer", "distress", "acquisition"}


# S3 helpers

def load_s3_json(bucket, key, default=None):
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            logger.info(f"S3 key not found (first run?): {key}")
            return default
        logger.warning(f"S3 load failed {key}: {e}")
        return default


def save_s3_json(bucket, key, data):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(data, ensure_ascii=False),
                  ContentType="application/json")


def load_held_company_ids():
    """Set of int company_ids held across all client portfolios in gracia-portfolios."""
    s3 = boto3.client("s3")
    held = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=PORTFOLIO_BUCKET, Prefix="portfolios/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    body = s3.get_object(Bucket=PORTFOLIO_BUCKET, Key=key)["Body"].read()
                    data = json.loads(body)
                except Exception as e:
                    logger.warning(f"portfolio read failed {key}: {e}")
                    continue
                for h in data.get("holdings", []):
                    cid = h.get("company_id")
                    if cid is None:
                        continue
                    try:
                        held.add(int(cid))
                    except (TypeError, ValueError):
                        continue
    except Exception as e:
        logger.warning(f"portfolio listing failed: {e}")
    logger.info(f"held company_ids across portfolios: {len(held)}")
    return held


def get_jwt():
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket="pipeline-token", Key="pipeline-jwt.json")
    return json.loads(obj["Body"].read())["jwt"]


# Pipeline API

def call_pipeline(method, endpoint, payload=None, jwt=None):
    base = "https://api.pipelinecrm.com/api/v3"
    url  = f"{base}{endpoint}"
    headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}
    data = json.dumps(payload).encode() if payload else None
    req  = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"status": r.status, "data": json.loads(r.read().decode())}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "data": e.read().decode()}
    except Exception as e:
        return {"status": 500, "data": str(e)}


# SES

def send_email(to, subject, body_text):
    ses = boto3.client("ses", region_name="us-east-1")
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [to]},
        Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body_text}}},
    )


# Web search

def _load_blocklist():
    """Seed UNION the editable S3 list (pipeline-token/source-blocklist.json,
    shape {"domains": [...]}). Falls back to the seed alone if the file is
    absent or unreadable."""
    data = load_s3_json(BLOCKLIST_BUCKET, BLOCKLIST_KEY, {}) or {}
    domains = data.get("domains", []) if isinstance(data, dict) else data
    extra = {str(d).strip().lower() for d in (domains or []) if d}
    return set(SOURCE_BLOCKLIST) | extra


def _blocked(url):
    """True if the result's host is on the active blocklist (domain or subdomain)."""
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return any(host == d or host.endswith("." + d) for d in _BLOCKLIST)
    except Exception:
        return False


def search_web(company_name, max_results=15):
    """Brave search across the catalyst event types. The freshness window is a
    computed {today-MAX_ANNOUNCEMENT_AGE_DAYS}to{today} range so the search only
    returns recently-published coverage - same window as the article-date gate.

    Over-fetches from Brave (count=20, the max) and then drops SOURCE_BLOCKLIST
    domains before returning, so the blocklist filtering doesn't starve the
    usable count. Returns up to max_results clean results."""
    today = datetime.now(timezone.utc)
    start = (today - timedelta(days=MAX_ANNOUNCEMENT_AGE_DAYS)).strftime("%Y-%m-%d")
    end   = today.strftime("%Y-%m-%d")
    fresh = f"{start}to{end}"
    query = f'"{company_name}" funding OR valuation OR raising OR IPO OR tender OR shutdown OR acquired'
    search_url = (
        f"https://api.search.brave.com/res/v1/web/search"
        f"?q={urllib.parse.quote(query)}&count=20&freshness={fresh}"
    )
    req = urllib.request.Request(search_url, headers={
        "Accept": "application/json",
        "X-Subscription-Token": BRAVE_API_KEY,
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        logger.warning(f"Brave search failed for '{company_name}': {e}")
        return []

    results = []
    for item in (data.get("web", {}).get("results") or []):
        url = item.get("url", "")
        if _blocked(url):
            logger.info(f"  blocked source: {url}")
            continue
        results.append({
            "title":   item.get("title", ""),
            "snippet": item.get("description", ""),
            "url":     url,
        })
        if len(results) >= max_results:
            break
    logger.info(f"  Brave: {len(results)} usable results for '{company_name}'")
    return results


# Bedrock analysis

def _extract_json(text):
    """Pull a JSON object out of a model reply that may carry prose around it.

    Strategy: direct parse; then a ```json fence; then scan for every
    brace-balanced {...} substring (string-aware) and return the best
    parseable one (prefers a candidate carrying the "found" key, else the
    longest). The prompt asks for bare JSON, so the direct parse almost
    always succeeds; the rest is the backstop.
    """
    if not text:
        return None
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    if "```" in text:
        for part in text.split("```"):
            p = part.strip()
            if p.lower().startswith("json"):
                p = p[4:].strip()
            if p.startswith("{"):
                try:
                    return json.loads(p)
                except Exception:
                    continue
    best = None
    n = len(text)
    for s in range(n):
        if text[s] != "{":
            continue
        depth = 0
        in_str = False
        esc = False
        for e in range(s, n):
            ch = text[e]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        cand = json.loads(text[s:e + 1])
                    except Exception:
                        cand = None
                    if isinstance(cand, dict):
                        if "found" in cand:
                            return cand
                        if best is None or len(text[s:e + 1]) > best[0]:
                            best = (len(text[s:e + 1]), cand)
                    break
    return best[1] if best else None


def analyze_results(company_name, search_results, company_summary=""):
    if not search_results:
        return {"found": False}

    snippets = "\n\n".join(
        f"[{i+1}] {r['title']}\n{r['snippet']}\nURL: {r['url']}"
        for i, r in enumerate(search_results)
    )

    summary = (company_summary or "").strip()
    facts_block = (
        f'\nWhat we know about "{company_name}" from our CRM (use this to confirm the result is about THIS company):\n{summary}\n'
        if summary else ""
    )

    prompt = f"""You are screening web results for ONE material, price-relevant catalyst for the private company "{company_name}". Investors hold or trade its shares on the secondary market.
{facts_block}
Search results:
{snippets}

Return found=true ONLY for one of these event types, and pick the SINGLE most material and most recent one:
- round_closed:      a NEW funding round that has closed / been announced, with terms (amount, valuation, or series).
- round_undisclosed: a NEW round has closed but the terms were not disclosed.
- round_targeting:   the company is reportedly raising, in talks, or targeting a new round that has NOT closed yet.
- ipo_announced:     an IPO filing, plan, or confirmed listing (capture any target valuation or price).
- tender_offer:      a company-run tender offer or share buyback for existing holders.
- distress:          shutdown, wind-down, insolvency, mass layoffs signalling distress, or a forced/down-round sale.
- acquisition:       the company is being acquired, in takeover/merger talks, or has received an acquisition offer (capture price if stated).

Return found=false if ANY of these apply:
- SOURCE QUALITY: it must be a primary news article, official press release, or the company's own post REPORTING a specific event. Reject aggregator/profile sites (Tracxn, PitchBook, Crunchbase, Wikipedia, ZoomInfo, Forge, premieralts, etc.); listicles/roundups ("top/best N", "startups you should know"); explainer or "what is" / "... explained" pages; personal blogs or Medium/Substack musings; and third-party "added to our index" / index-inclusion promos. None of those qualify no matter what they claim.
- NO DATE: if the item has no clear date (ongoing/forward), accept it ONLY if it is from a recognised primary news outlet (Reuters, Bloomberg, CNBC, WSJ, FT, The Information, TechCrunch, CoinDesk, and the like). Undated and not from such an outlet -> reject.
- WRONG COMPANY: it is about a different company than ours. A shared or similar name is NOT enough — the business in the result must match what we know about "{company_name}" (see the description above, if provided). If the result describes a company in a clearly different line of business (e.g. a carmaker when ours is a defense-software startup), return found=false even if the names overlap.
- It is only product news, hiring, a partnership, an award, or generic PR.
- STALE COVERAGE: the article/coverage itself must be recent - published within the last {MAX_ANNOUNCEMENT_AGE_DAYS} days - OR a clearly ongoing/forward situation. An older event (a tender or round from months ago) IS fine as long as the coverage reporting it is recent: what must be fresh is the article, not the event. Old coverage and not ongoing -> reject.

IPO chatter (timing, "exploring an IPO", "charting a path to IPO") IS acceptable, but only from a reputable primary news outlet - not an analysis/opinion aggregator or SEO/contributor hub.
Only state figures explicitly present in the result. Never infer or estimate a valuation or amount; if a figure is not stated, omit it.

Output ONLY the JSON object, starting immediately at the opening brace. No reasoning, no preface, no markdown:
{{
  "found": true or false,
  "event_type": "round_closed" | "round_undisclosed" | "round_targeting" | "ipo_announced" | "tender_offer" | "distress" | "acquisition" | null,
  "is_current": true if this is an ongoing/forward situation that may not carry a hard date (targeting/in talks), else false,
  "confirmed": true if officially confirmed, false if reported/rumored,
  "headline": "article title or null",
  "url": "article URL or null",
  "catalyst_line": "ONE short sentence, max ~12 words, for a portfolio readout. Examples: 'Reportedly targeting $4B raise', 'Confidential IPO filing, ~$1.5T target', 'In takeover talks with Intel, ~$5B', 'Tender offer announced for employee shares', 'Reports of wind-down'. For a closed round use 'Closed $XB round'. Or null.",
  "round_series": "e.g. Series F, or null",
  "valuation_usd": number or null,
  "valuation_basis": "pre" | "post" | null,
  "raise_amount_usd": number or null,
  "pps_usd": number or null,
  "article_date": "YYYY-MM-DD or null",
  "round_date": "YYYY-MM-DD close date ONLY if explicitly stated, else null"
}}"""

    bedrock = boto3.client(
        "bedrock-runtime", region_name="us-east-1",
        config=BotoConfig(retries={"max_attempts": 8, "mode": "adaptive"}),
    )
    try:
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 700,
                "messages": [
                    {"role": "user", "content": prompt},
                ],
            }),
            contentType="application/json",
            accept="application/json",
        )
        raw  = json.loads(response["body"].read())
        text = raw["content"][0]["text"]
    except Exception as e:
        logger.warning(f"Bedrock call failed for {company_name}: {e} - skipping")
        return {"found": False}
    parsed = _extract_json(text)

    if parsed is None:
        logger.warning(f"Bedrock parse failed for {company_name} - raw: {text[:300]}")
        return {"found": False}
    return parsed


# Dates & gating

def _parse_date(s):
    if not s:
        return None
    s = str(s).strip().replace("/", "-")
    try:
        parts = s.split("-")
        y = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 1
        d = int(parts[2]) if len(parts) > 2 else 1
        return datetime(y, m, d, tzinfo=timezone.utc)
    except Exception:
        return None


def _fmt_month_year(s):
    """ISO date string -> "Mar '26". Empty string if unparseable."""
    dt = _parse_date(s)
    return dt.strftime("%b '%y") if dt else ""


def _is_recent(date_str):
    dt = _parse_date(date_str)
    if dt is None:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(days=MAX_ANNOUNCEMENT_AGE_DAYS)


def passes_date_gate(analysis):
    """Gate on the ARTICLE's publication date, never the event date. A recent
    article about an older tender/round is a valid, durable price mark - only the
    coverage has to be fresh. is_current only rescues TRULY undated items: a
    present-but-old article date is rejected even for a forward/ongoing situation
    (kills stale 'in talks' pages that would otherwise ride the is_current flag)."""
    art = analysis.get("article_date")
    if art:
        return _is_recent(art)                 # dated coverage must be recent, period
    return bool(analysis.get("is_current"))     # only truly undated items lean on is_current


def get_cf(record, field):
    if not record:
        return None
    return (record.get("custom_fields") or {}).get(field)


def _lr_has_caught_up(record, round_date):
    """True if the LR column already reflects this round, so we should NOT echo it in Catalyst."""
    lr_dt = _parse_date(get_cf(record, LR_DATE_FIELD))
    if lr_dt is None:
        return False
    rd = _parse_date(round_date)
    if rd is None:
        # Round had no stated close date but LR has a date - assume Kate is tracking it.
        return True
    return lr_dt >= rd - timedelta(days=2)   # small tolerance for date drift


def _round_pending_line(analysis):
    amt = analysis.get("raise_amount_usd")
    amt_str = fmt_usd(amt) if amt else (analysis.get("round_series") or "new")
    rd = _fmt_month_year(analysis.get("round_date") or analysis.get("article_date") or "")
    date_part = f" {rd}" if rd else ""
    return f"Closed {amt_str} round{date_part} {PENDING_MARKER}".strip()


def desired_catalyst(analysis, record):
    """Returns (value_or_None, kind). value None = no Catalyst write from this detection."""
    et = analysis.get("event_type")
    if et in NONROUND_EVENTS:
        line = (analysis.get("catalyst_line") or "").strip()
        return (line or None), "nonround"
    if et in ROUND_EVENTS:
        if _lr_has_caught_up(record, analysis.get("round_date")):
            return None, "round_in_lr"
        return _round_pending_line(analysis), "round_pending"
    return None, "none"


def stale_pending_clear(record):
    """If the stored Catalyst is a round-pending line and the LR column has since
    caught up, return '' to clear it. Else None (no change). Reads the round date
    out of either the old ISO format "(YYYY-MM-DD)" or the new "Mon 'YY" format."""
    stored = (get_cf(record, CATALYST_FIELD) or "").strip()
    if PENDING_MARKER not in stored:
        return None
    rd = None
    m = re.search(r"\((\d{4}-\d{2}-\d{2})\)", stored)          # old ISO format
    if m:
        rd = m.group(1)
    else:
        m2 = re.search(r"([A-Z][a-z]{2}) '(\d{2})", stored)    # new "Mon '26" format
        if m2:
            try:
                mo = datetime.strptime(m2.group(1), "%b").month
                rd = f"20{m2.group(2)}-{mo:02d}-01"
            except Exception:
                rd = None
    if _lr_has_caught_up(record, rd):
        return ""
    return None


# Format helpers

def fmt_usd(v):
    if v is None or v == "" or v == 0:
        return "-"
    try:
        f = float(v)
        if f >= 1_000_000_000:
            return f"${f / 1_000_000_000:.2f}B"
        if f >= 1_000_000:
            return f"${f / 1_000_000:.1f}M"
        return f"${f:,.2f}"
    except Exception:
        return str(v)


def pipeline_url(company_id):
    return f"https://app.pipelinedeals.com/companies/{company_id}"


# Universe (union: held portfolios + deal tiers)

def build_company_queue(all_deals, all_companies, held_ids, company_index, today):
    # In DRY_RUN (harvest) run every tier and ignore the per-tier caps, so a dry
    # run is a complete sweep. Live runs keep the normal weekly/monthly schedule.
    run_tier2 = (today.weekday() == 0) or DRY_RUN
    run_tier3 = (today.day == 1) or DRY_RUN

    # Held companies - top priority, scanned first, cap-exempt.
    queue, seen_ids = [], set()
    for cid in held_ids:
        rec  = company_index.get(cid)
        name = rec.get("name") if rec else None
        if not name:
            continue
        queue.append((cid, name, 0, rec))
        seen_ids.add(cid)

    # Deal-driven tiers.
    company_deals = {}
    for deal in all_deals:
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id == MATCHED_STAGE:
            continue
        co      = deal.get("company") or {}
        co_id   = co.get("id") or deal.get("company_id")
        co_name = co.get("name") or deal.get("company_name", "")
        if not co_id or not co_name:
            continue
        d = company_deals.setdefault(co_id, {"name": co_name, "firm": 0, "inquiry": 0, "has_other": False})
        if stage_id == FIRM_STAGE:
            d["firm"] += 1
        elif stage_id == INQUIRY_STAGE:
            d["inquiry"] += 1
        elif stage_id in (HOLD_STAGE, CONFIRM_STAGE):
            d["has_other"] = True

    tier1, tier2 = [], []
    for co_id, info in company_deals.items():
        if co_id in seen_ids:
            continue
        rec = company_index.get(co_id)
        if info["firm"] >= 1 or (info["firm"] + info["inquiry"]) >= 3:
            tier1.append((co_id, info["name"], 1, rec)); seen_ids.add(co_id)
        elif info["firm"] > 0 or info["inquiry"] > 0 or info["has_other"]:
            tier2.append((co_id, info["name"], 2, rec)); seen_ids.add(co_id)

    tier3 = []
    if run_tier3:
        for co in all_companies:
            co_id = co.get("id")
            if not co_id or co_id in seen_ids:
                continue
            org = (co.get("custom_fields") or {}).get(ORG_TYPE_FIELD, [])
            if not isinstance(org, list):
                org = [org]
            if TRADED_ISSUER_ID in org:
                tier3.append((co_id, co.get("name", ""), 3, co))

    logger.info(
        f"held={len(queue)} tier1={len(tier1)} "
        f"tier2={len(tier2)}(run={run_tier2}) tier3={len(tier3)}(run={run_tier3})"
    )

    cap1 = None if DRY_RUN else TIER1_CAP
    cap2 = None if DRY_RUN else TIER2_CAP
    cap3 = None if DRY_RUN else TIER3_CAP
    out = queue + tier1[:cap1]
    if run_tier2:
        out += tier2[:cap2]
    if run_tier3:
        out += tier3[:cap3]
    return out


# Main handler

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    logger.info(f"Catalyst scanner started  DRY_RUN={DRY_RUN}  {now.isoformat()}")

    if not BRAVE_API_KEY:
        logger.error("BRAVE_API_KEY env var not set - aborting")
        return {"statusCode": 500, "error": "BRAVE_API_KEY not configured"}

    global _BLOCKLIST
    _BLOCKLIST = _load_blocklist()
    logger.info(f"blocklist: {len(_BLOCKLIST)} domains")

    jwt = get_jwt() if not DRY_RUN else None

    deals_data     = load_s3_json(CACHE_BUCKET, "deals.json",         {"deals": []})
    companies_data = load_s3_json(CACHE_BUCKET, "companies.json",     {"companies": []})
    seen_data      = load_s3_json(CACHE_BUCKET, "seen-articles.json", {"urls": []})
    held_ids       = load_held_company_ids()

    all_deals     = deals_data.get("deals", [])
    all_companies = companies_data.get("companies", [])
    seen_urls     = set(seen_data.get("urls", []))
    new_seen_urls = set()

    company_index = {c["id"]: c for c in all_companies if c.get("id") is not None}

    queue = build_company_queue(all_deals, all_companies, held_ids, company_index, now)
    logger.info(f"Search queue: {len(queue)} companies (held={len(held_ids)})")

    alerts          = []
    catalyst_writes = []

    for co_id, co_name, tier, record in queue:
        if context and context.get_remaining_time_in_millis() < TIMEOUT_BUFFER_MS:
            logger.warning("Approaching Lambda timeout - stopping early")
            break

        logger.info(f"[tier {tier}] {co_name}")

        # Cheap self-clear: a stored round-pending line whose round the LR column now reflects.
        clear_val = stale_pending_clear(record)

        results    = search_web(co_name)
        co_summary = (record or {}).get("description") or ""
        analysis   = analyze_results(co_name, results, company_summary=co_summary) if results else {"found": False}

        desired, kind = None, "none"
        if analysis.get("found") and passes_date_gate(analysis):
            url = analysis.get("url") or ""
            # De-dup only on live runs; a DRY_RUN harvest always shows the full
            # picture instead of being thinned by prior live history.
            if url and url in seen_urls and not DRY_RUN:
                logger.info(f"  already reported: {url}")
            else:
                desired, kind = desired_catalyst(analysis, record)
                if url:
                    new_seen_urls.add(url)

        stored      = (get_cf(record, CATALYST_FIELD) or "").strip()
        final_value = None  # None = leave Catalyst unchanged
        is_clear    = False
        if desired is not None:
            if desired != stored:
                final_value = desired
        elif clear_val is not None and clear_val != stored:
            final_value = clear_val
            is_clear = True

        if final_value is None:
            continue

        alerts.append({
            "company": co_name, "co_id": co_id, "tier": tier,
            "is_clear": is_clear, "kind": kind,
            "catalyst": final_value if final_value else "(cleared)",
            "event_type": ("cleared" if is_clear else analysis.get("event_type")),
            "confirmed": analysis.get("confirmed"),
            "headline": "" if is_clear else (analysis.get("headline") or ""),
            "url": "" if is_clear else (analysis.get("url") or ""),
            "date": "" if is_clear else (analysis.get("round_date") or analysis.get("article_date") or ""),
            "round_series": None if is_clear else analysis.get("round_series"),
            "valuation": None if is_clear else (fmt_usd(analysis.get("valuation_usd")) if analysis.get("valuation_usd") else None),
            "valuation_basis": analysis.get("valuation_basis"),
            "raise_amount": None if is_clear else (fmt_usd(analysis.get("raise_amount_usd")) if analysis.get("raise_amount_usd") else None),
            "pps": None if is_clear else (fmt_usd(analysis.get("pps_usd")) if analysis.get("pps_usd") else None),
        })
        catalyst_writes.append((co_id, co_name, final_value))
        logger.info(f"  Catalyst: {final_value!r}")

        if not DRY_RUN and co_id:
            result = call_pipeline(
                "PUT", f"/companies/{co_id}.json",
                {"company": {"custom_fields": {CATALYST_FIELD: final_value}}},
                jwt=jwt,
            )
            if result["status"] == 200:
                logger.info(f"  Pipeline updated for {co_name}")
            else:
                logger.error(f"  Pipeline update failed for {co_name}: {result}")

    if new_seen_urls and not DRY_RUN:
        save_s3_json(CACHE_BUCKET, "seen-articles.json",
                     {"urls": list(seen_urls | new_seen_urls)})
        logger.info(f"Saved {len(new_seen_urls)} new URLs to seen-articles.json")

    # Summary email -> Chad and Kate
    dry_tag = "  [DRY RUN - nothing written to CRM]" if DRY_RUN else ""
    alerts.sort(key=lambda a: a.get("date") or "", reverse=True)

    lines = [
        f"CATALYST SCANNER - {now.strftime('%Y-%m-%d')}{dry_tag}",
        f"Scanned: {len(queue)}   Catalyst updates: {len(catalyst_writes)}",
        "",
    ]
    if alerts:
        for a in alerts:
            lines.append(f"-- {a['company']}  (tier {a['tier']}) --------------------")
            if a["is_clear"]:
                lines += ["  Cleared stale round-pending line (LR column now reflects it)"]
            else:
                conf = "confirmed" if a["confirmed"] else "reported"
                lines += [
                    f"  Catalyst -> {a['catalyst']}",
                    f"  Type: {a['event_type']} ({conf})",
                    f"  {a['headline']}",
                    f"  {a['date'] or 'date n/a'}   {a['url']}",
                ]
                if a["kind"] == "round_pending":
                    det = []
                    if a["round_series"]:
                        s = str(a["round_series"])
                        det.append(s if s.lower().startswith("series") else f"Series {s}")
                    if a["valuation"]:    det.append(f"val {a['valuation']} ({a['valuation_basis'] or 'basis?'})")
                    if a["raise_amount"]: det.append(f"raise {a['raise_amount']}")
                    if a["pps"]:          det.append(f"PPS {a['pps']}")
                    lines.append("  FOR KATE -> update LR column: " + (", ".join(det) if det else "details pending"))
            lines += [f"  Pipeline: {pipeline_url(a['co_id'])}", ""]
    else:
        lines.append("No new catalysts found today.")

    summary = "\n".join(lines)
    logger.info(summary)

    subject = (f"{'[DRY RUN] ' if DRY_RUN else ''}Catalyst Scanner - "
               f"{now.strftime('%Y-%m-%d')} ({len(catalyst_writes)} updates)")
    for recipient in [CHAD_EMAIL, KATE_EMAIL]:
        try:
            send_email(recipient, subject, summary)
            logger.info(f"Summary emailed to {recipient}")
        except Exception as e:
            logger.error(f"Failed to email {recipient}: {e}")

    return {
        "statusCode": 200,
        "dry_run": DRY_RUN,
        "scanned": len(queue),
        "catalyst_updates": len(catalyst_writes),
    }
