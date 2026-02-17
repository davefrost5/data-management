"""
Opendorse NIL Marketplace Scraper
Fetches athlete deal value ranges and social media handles from Opendorse.com

Respects robots.txt (avoids /search*), uses conservative delays, and handles
rate limiting to avoid blocking.
"""

import csv
import json
import random
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://opendorse.com"
# Next.js build ID - may need to be updated if site redeploys (extract from page source)
NEXT_BUILD_ID = "2JEdexce9Z4mTlPF71ejk"

# Rate limiting - conservative to avoid blocking (robots.txt allows / but be respectful)
MIN_DELAY = 2.0   # minimum seconds between requests
MAX_DELAY = 4.0   # maximum seconds (adds random jitter)
BATCH_PAUSE_EVERY = 15   # pause longer every N profile requests
BATCH_PAUSE_SECS = 30    # seconds to pause between batches
RETRY_AFTER_DEFAULT = 60  # if 429 received and no Retry-After header

# Full scrape mode - even more conservative (many teams = many requests)
TEAM_PAUSE_SECS = 10     # pause between finishing one team and starting next


def _delay():
    """Random delay between requests to avoid detection patterns."""
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def _batch_pause(request_count: int) -> None:
    """Longer pause every N requests to mimic human browsing."""
    if request_count > 0 and request_count % BATCH_PAUSE_EVERY == 0:
        print(f"    [Pausing {BATCH_PAUSE_SECS}s after {request_count} requests...]")
        time.sleep(BATCH_PAUSE_SECS)


def _request_with_retry(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    """Make request, respecting 429 rate limits and Retry-After header."""
    while True:
        r = session.get(url, params=params, timeout=20)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", RETRY_AFTER_DEFAULT))
            print(f"    [Rate limited (429). Waiting {retry_after}s before retry...]")
            time.sleep(retry_after)
            continue
        return r


def get_session():
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://opendorse.com/",
    })
    return session


def discover_build_id(session: requests.Session) -> str:
    """Extract Next.js build ID from the main page (from __NEXT_DATA__ script)."""
    try:
        r = _request_with_retry(session, BASE_URL)
        r.raise_for_status()
        # __NEXT_DATA__ contains buildId - most reliable source
        match = re.search(r'"buildId"\s*:\s*"([^"]+)"', r.text)
        if match:
            bid = match.group(1)
            # Build IDs are typically 20+ chars, avoid short false matches like "media"
            if len(bid) >= 15:
                return bid
        # Fallback: look for _next/data/BUILDID/ in script/link tags
        match = re.search(r'/_next/data/([a-zA-Z0-9]{15,})/', r.text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return NEXT_BUILD_ID


def fetch_teams(session: requests.Session, build_id: str) -> list[dict]:
    """Fetch all teams from the browse/teams JSON endpoint."""
    teams = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = f"{BASE_URL}/_next/data/{build_id}/browse/teams.json"
        params = {"slug": "teams", "page": page} if page > 1 else {"slug": "teams"}
        try:
            r = _request_with_retry(session, url, params)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"Error fetching teams page {page}: {e}")
            break

        marketplaces = data.get("pageProps", {}).get("initialState", {}).get("marketplaces", {})
        browse = marketplaces.get("browseChildMarketplaces", [])
        summary = marketplaces.get("getMarketplaceTypeSummary", {})
        total_pages = summary.get("totalPages", 1)

        for team in browse:
            teams.append({
                "id": team.get("id"),
                "slug": team.get("slug"),
                "name": team.get("name"),
            })

        print(f"  Fetched teams page {page}/{total_pages} ({len(teams)} teams so far)")
        page += 1
        _delay()

    return teams


def fetch_team_athletes(session: requests.Session, build_id: str, team_slug: str) -> list[dict]:
    """Fetch all athletes for a team (handles pagination)."""
    athletes = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = f"{BASE_URL}/_next/data/{build_id}/{team_slug}.json"
        params = {"slug": team_slug, "athlete_page": page}
        try:
            r = _request_with_retry(session, url, params)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"    Error fetching athletes page {page}: {e}")
            break

        search = data.get("pageProps", {}).get("initialState", {}).get("search", {})
        athlete_list = search.get("athletes", [])
        summary = search.get("searchAthletesSummary", {})
        total_pages = summary.get("athlete_totalPages", 1)

        for a in athlete_list:
            athletes.append({
                "accountId": a.get("accountId"),
                "firstName": a.get("firstName"),
                "lastName": a.get("lastName"),
                "networkProfileCode": a.get("networkProfileCode"),
                "sports": a.get("sports", []),
                "currentTeams": a.get("currentTeams", []),
                "instagramReach": a.get("instagramReach"),
                "twitterReach": a.get("twitterReach"),
                "tikTokReach": a.get("tikTokReach"),
            })

        page += 1
        _delay()

    return athletes


def parse_profile_page(html: str) -> dict:
    """
    Parse athlete profile HTML for deal value ranges and social media handles.
    """
    result = {
        "social_handles": {},
        "deal_ranges": [],
    }

    soup = BeautifulSoup(html, "html.parser")

    # Social media links - look for instagram, tiktok, twitter, facebook, linkedin
    social_domains = {
        "instagram.com": "instagram",
        "tiktok.com": "tiktok",
        "twitter.com": "twitter",
        "x.com": "twitter",
        "facebook.com": "facebook",
        "linkedin.com": "linkedin",
    }

    # Exclude Opendorse's own links (footer: facebook.com/opendorse, linkedin.com/company/opendorse)
    opendorse_patterns = ("opendorse", "/company/opendorse", "opendorse/")

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if any(p in href.lower() for p in opendorse_patterns):
            continue
        for domain, platform in social_domains.items():
            if domain in href and platform not in result["social_handles"]:
                result["social_handles"][platform] = href.strip()
                break

    # Deal ranges: capture each service type with its header, only if it has a price range
    # Known service names on Opendorse profiles
    service_names = (
        r"Social Posts|Appearance|Photo/Video Creation|Photo/Video/Audio Creation|"
        r"Autograph Signing|Keynote Speech|Demonstration|Instagram Post|Twitter Post|"
        r"TikTok Post|Facebook Post|LinkedIn Post|Podcast|Production Shoot|Youtube Post"
    )
    # Match: "ServiceName" followed by "$X - $Y" (only include if range exists)
    service_range_pattern = re.compile(
        rf"({service_names})\s*(\$[\d,]+\s*-\s*\$[\d,]+)",
        re.I,
    )

    text = soup.get_text(separator=" ")
    for match in service_range_pattern.finditer(text):
        service = match.group(1).strip()
        range_str = match.group(2).strip()
        # Dedupe by service name (same service may appear multiple times in HTML)
        if not any(r.get("service") == service for r in result["deal_ranges"]):
            result["deal_ranges"].append({"service": service, "range": range_str})

    return result


def fetch_athlete_profile(session: requests.Session, profile_code: str) -> dict | None:
    """Fetch and parse an athlete's profile page for deal values and social handles."""
    url = f"{BASE_URL}/profile/{profile_code}"
    try:
        r = _request_with_retry(session, url)
        r.raise_for_status()
        return parse_profile_page(r.text)
    except Exception as e:
        print(f"      Error fetching profile {profile_code}: {e}")
        return None


def scrape_team(
    session: requests.Session,
    build_id: str,
    team_slug: str,
    team_name: str,
    fetch_profiles: bool = True,
    max_athletes: int | None = None,
) -> list[dict]:
    """
    Scrape a single team: get athletes and optionally their profile details.
    """
    athletes = fetch_team_athletes(session, build_id, team_slug)
    if max_athletes:
        athletes = athletes[:max_athletes]

    results = []
    request_count = 0
    for i, athlete in enumerate(athletes):
        profile_code = athlete.get("networkProfileCode")
        if not profile_code:
            continue

        record = {
            "name": f"{athlete.get('firstName', '')} {athlete.get('lastName', '')}".strip(),
            "profile_code": profile_code,
            "profile_url": f"{BASE_URL}/profile/{profile_code}",
            "sports": athlete.get("sports", []),
            "teams": athlete.get("currentTeams", []),
            "social_handles": {},
            "deal_ranges": [],
        }

        if fetch_profiles:
            _batch_pause(request_count)
            profile_data = fetch_athlete_profile(session, profile_code)
            request_count += 1
            if profile_data:
                record["social_handles"] = profile_data.get("social_handles", {})
                record["deal_ranges"] = profile_data.get("deal_ranges", [])
            _delay()

        results.append(record)
        if (i + 1) % 5 == 0:
            print(f"    Processed {i + 1}/{len(athletes)} athletes")

    return results


def scrape_all_teams(
    session: requests.Session,
    build_id: str,
    output_dir: str = "opendorse_data",
    max_teams: int | None = None,
    max_athletes_per_team: int | None = None,
    fetch_profiles: bool = True,
    resume: bool = False,
) -> dict:
    """
    Scrape all teams from Opendorse. Saves incrementally (per team) so progress
    isn't lost. Outputs JSON (by team) + flat CSV for easy filtering.
    With --resume, skips teams that already have a team_{slug}.json file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    teams = fetch_teams(session, build_id)
    if max_teams:
        teams = teams[:max_teams]

    # Resume: skip teams we already have
    if resume:
        skipped = []
        teams_to_scrape = []
        for t in teams:
            slug = t.get("slug")
            if slug and (output_path / f"team_{slug}.json").exists():
                skipped.append(t.get("name"))
            else:
                teams_to_scrape.append(t)
        if skipped:
            print(f"Resuming: skipping {len(skipped)} already-scraped teams")
            for n in skipped[:5]:
                print(f"  - {n}")
            if len(skipped) > 5:
                print(f"  ... and {len(skipped) - 5} more")
        teams = teams_to_scrape

    all_data = {"teams": [], "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    csv_path = output_path / "athletes_all.csv"
    csv_append = resume and csv_path.exists()
    csv_file = open(csv_path, "a" if csv_append else "w", newline="", encoding="utf-8")
    csv_writer = None
    csv_header_written = csv_append  # when appending, header already exists

    total_athletes = 0
    for i, team in enumerate(teams):
        slug = team.get("slug")
        name = team.get("name")
        if not slug:
            continue

        print(f"\n[{i + 1}/{len(teams)}] {name} ({slug})")
        _delay()

        athletes = scrape_team(
            session, build_id, slug, name,
            fetch_profiles=fetch_profiles,
            max_athletes=max_athletes_per_team,
        )

        team_block = {"team": {"slug": slug, "name": name}, "athletes": athletes}
        all_data["teams"].append(team_block)
        total_athletes += len(athletes)

        # Save team JSON incrementally
        team_file = output_path / f"team_{slug}.json"
        with open(team_file, "w", encoding="utf-8") as f:
            json.dump(team_block, f, indent=2, ensure_ascii=False)

        # Append to flat CSV (all deal types: structured columns + full list)
        deal_columns = [
            "deal_social_posts",
            "deal_appearance",
            "deal_photo_video_creation",
            "deal_autograph_signing",
            "deal_keynote_speech",
        ]
        service_to_col = {
            "social posts": "deal_social_posts",
            "appearance": "deal_appearance",
            "photo/video creation": "deal_photo_video_creation",
            "photo/video/audio creation": "deal_photo_video_creation",
            "autograph signing": "deal_autograph_signing",
            "keynote speech": "deal_keynote_speech",
        }
        base_cols = ["name", "team", "team_slug", "sport", "profile_url", "instagram", "tiktok", "twitter"]
        all_cols = base_cols + deal_columns + ["deal_ranges_all"]

        for a in athletes:
            deals = {col: "" for col in deal_columns}
            ranges_parts = []
            for dr in a.get("deal_ranges", []):
                svc = (dr.get("service") or "").strip()
                rng = (dr.get("range") or "").strip()
                if svc and rng:
                    ranges_parts.append(f"{svc}: {rng}")
                    svc_lower = svc.lower()
                    col = service_to_col.get(svc_lower)
                    if col and col in deals:
                        deals[col] = rng
            row = {
                "name": a.get("name", ""),
                "team": name,
                "team_slug": slug,
                "sport": ", ".join(a.get("sports", [])),
                "profile_url": a.get("profile_url", ""),
                "instagram": a.get("social_handles", {}).get("instagram", ""),
                "tiktok": a.get("social_handles", {}).get("tiktok", ""),
                "twitter": a.get("social_handles", {}).get("twitter", ""),
                **deals,
                "deal_ranges_all": " | ".join(ranges_parts),
            }
            if csv_writer is None:
                csv_writer = csv.DictWriter(csv_file, fieldnames=all_cols, extrasaction="ignore")
                if not csv_header_written:
                    csv_writer.writeheader()
            csv_writer.writerow(row)

        # Pause between teams
        if i < len(teams) - 1:
            print(f"    Pausing {TEAM_PAUSE_SECS}s before next team...")
            time.sleep(TEAM_PAUSE_SECS)

    csv_file.close()

    # Save combined JSON (merge existing team files when resuming)
    combined_path = output_path / "all_teams.json"
    if resume:
        existing = {}
        for tf in output_path.glob("team_*.json"):
            try:
                with open(tf, encoding="utf-8") as f:
                    data = json.load(f)
                    slug = data.get("team", {}).get("slug")
                    if slug:
                        existing[slug] = data
            except Exception:
                pass
        # Merge: existing + newly scraped (new overwrites if same slug)
        for tb in all_data["teams"]:
            slug = tb.get("team", {}).get("slug")
            if slug:
                existing[slug] = tb
        all_data["teams"] = list(existing.values())
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    total_from_files = sum(len(t.get("athletes", [])) for t in all_data["teams"])
    print(f"\nDone. Scraped {total_athletes} athletes this run. Total in output: {total_from_files} from {len(all_data['teams'])} teams.")
    print(f"  JSON (by team): {output_path}/team_*.json")
    print(f"  JSON (combined): {combined_path}")
    print(f"  CSV (flat): {csv_path}")
    return all_data


def main():
    """Main entry point - scrape Alabama as a demo."""
    print("Opendorse NIL Scraper")
    print("=" * 50)

    session = get_session()

    print("Discovering build ID...")
    build_id = discover_build_id(session)
    if build_id == "media" or len(build_id) < 15:
        build_id = NEXT_BUILD_ID
        print(f"Using fallback build ID: {build_id}")
    else:
        print(f"Using build ID: {build_id}")

    _delay()  # pause before starting team scrape

    # Demo: scrape Alabama Crimson Tide
    team_slug = "alabama-crimsontide"
    team_name = "Alabama Crimson Tide"
    print(f"\nScraping {team_name} (max 10 athletes for demo)...")

    results = scrape_team(
        session,
        build_id,
        team_slug,
        team_name,
        fetch_profiles=True,
        max_athletes=10,
    )

    print(f"\nScraped {len(results)} athletes")
    print("\nSample output:")
    for r in results[:3]:
        print(json.dumps(r, indent=2))
        print("-" * 40)

    # Save to JSON
    out_path = "opendorse_alabama_sample.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nFull results saved to {out_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Opendorse NIL athlete data")
    parser.add_argument("--all", action="store_true", help="Scrape all teams (not just Alabama demo)")
    parser.add_argument("--max-teams", type=int, default=None, help="Limit number of teams (e.g. 5 for testing)")
    parser.add_argument("--max-athletes", type=int, default=None, help="Max athletes per team")
    parser.add_argument("--output-dir", default="opendorse_data", help="Output directory for --all mode")
    parser.add_argument("--no-profiles", action="store_true", help="Skip profile pages (faster, no social/deal data)")
    parser.add_argument("--resume", action="store_true", help="Skip teams that already have team_*.json (continue after crash)")
    args = parser.parse_args()

    if args.all:
        print("Opendorse NIL Scraper - FULL SCRAPE MODE")
        print("=" * 50)
        print("Respects robots.txt, uses conservative delays.")
        print("=" * 50)
        session = get_session()
        build_id = discover_build_id(session)
        if build_id == "media" or len(build_id) < 15:
            build_id = NEXT_BUILD_ID
        print(f"Build ID: {build_id}")
        _delay()
        scrape_all_teams(
            session,
            build_id,
            output_dir=args.output_dir,
            max_teams=args.max_teams,
            max_athletes_per_team=args.max_athletes,
            fetch_profiles=not args.no_profiles,
            resume=args.resume,
        )
    else:
        main()
