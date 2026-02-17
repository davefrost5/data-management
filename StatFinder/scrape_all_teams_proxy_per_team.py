import argparse
import asyncio
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# -------------------------
# Headers / UA
# -------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
]

BLOCK_STRINGS = [
    "errors.edgesuite.net",
    "reference #",
    "access denied",
    "you don't have permission to access",
    "request blocked",
    "incident id",
]


def detect_block(text: str) -> bool:
    t = (text or "").lower()

    # Fast must-match for real Akamai block pages
    if "errors.edgesuite.net" in t:
        return True
    if "you don't have permission to access" in t:
        return True
    if "access denied" in t and "ncaa statistics" not in t:
        return True
    if "reference #" in t and ("denied" in t or "permission" in t):
        return True
    if "request blocked" in t:
        return True
    if "incident id" in t and ("blocked" in t or "denied" in t):
        return True

    return False


def random_headers() -> Dict[str, str]:
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


# -------------------------
# Team list loading
# -------------------------
def load_team_list(team_list_path: str, division_filter: Optional[str]) -> List[Dict[str, str]]:
    data = json.loads(Path(team_list_path).read_text())

    # nested dict format: { "D3": { "Conference": [ {team_id,name}, ...] } }
    if isinstance(data, dict):
        teams = []
        for division, conferences in data.items():
            if division_filter and division.upper() != division_filter.upper():
                continue
            if isinstance(conferences, dict):
                for _, team_list in conferences.items():
                    if isinstance(team_list, list):
                        for t in team_list:
                            teams.append(
                                {
                                    "id": str(t.get("team_id") or t.get("id")),
                                    "name": t.get("name") or t.get("team_name") or "Unknown",
                                }
                            )
        if teams:
            return teams

    # flat list format: [ {team_id,name}, ... ]
    teams = []
    for t in data:
        teams.append(
            {
                "id": str(t.get("team_id") or t.get("id")),
                "name": t.get("name") or t.get("team_name") or "Unknown",
            }
        )
    return teams


# -------------------------
# Proxy loading + rotation
# -------------------------
def load_proxies(proxy_file: Optional[str]) -> List[str]:
    if not proxy_file:
        return []
    p = Path(proxy_file)
    if not p.exists():
        raise FileNotFoundError(f"Proxy file not found: {proxy_file}")
    proxies = [line.strip() for line in p.read_text().splitlines() if line.strip()]
    return proxies


@dataclass
class ProxyRotator:
    proxies: List[str]
    idx: int = 0

    def next(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self.idx % len(self.proxies)]
        self.idx += 1
        return proxy


def parse_proxy(proxy_line: str) -> Dict[str, Any]:
    """
    Returns Playwright proxy dict with separated credentials:
      {"server": "http://host:port", "username": "...", "password": "..."}

    Accepts either:
      - http://user:pass@host:port
      - user:pass@host:port
      - host:port:user:pass
      - host:port (no auth)
    """
    s = proxy_line.strip()

    # host:port:user:pass -> to URL
    if not s.startswith("http://") and not s.startswith("https://") and s.count(":") == 3 and "@" not in s:
        host, port, user, pw = s.split(":")
        s = f"http://{user}:{pw}@{host}:{port}"

    # user:pass@host:port -> add scheme
    if "@" in s and not s.startswith("http://") and not s.startswith("https://"):
        s = "http://" + s

    # host:port -> add scheme
    if "@" not in s and not s.startswith("http://") and not s.startswith("https://") and ":" in s:
        s = "http://" + s

    u = urlparse(s)
    if not u.hostname or not u.port:
        raise ValueError(f"Invalid proxy line: {proxy_line}")

    server = f"{u.scheme}://{u.hostname}:{u.port}"
    proxy_cfg = {"server": server}

    # Only include creds if present (avoids weird empty-user issues on some gateways)
    if u.username:
        proxy_cfg["username"] = u.username
    if u.password:
        proxy_cfg["password"] = u.password

    return proxy_cfg


# -------------------------
# DOM extraction
# -------------------------
async def extract_player_table_from_dom(page) -> Tuple[List[str], List[Dict[str, Any]]]:
    js = r"""
    () => {
      function text(el) {
        return (el && el.textContent ? el.textContent : "").trim();
      }
      function cellValue(td) {
        const dataOrder = td.getAttribute("data-order");
        if (dataOrder !== null) return dataOrder.trim();
        const innerDiv = td.querySelector("div");
        const divText = innerDiv ? text(innerDiv) : "";
        if (divText) return divText;
        return text(td);
      }

      let table = document.querySelector("div.dataTables_scrollBody table#stat_grid") ||
                  document.querySelector("table#stat_grid");

      if (!table) return { headers: [], players: [] };

      let headerEls = table.querySelectorAll("thead tr th div.dataTables_sizing");
      if (!headerEls || headerEls.length === 0) headerEls = table.querySelectorAll("thead tr th");
      const rawHeaders = Array.from(headerEls).map(h => text(h));

      const seen = {};
      const headers = rawHeaders.map(h => {
        const base = h || "col";
        if (seen[base]) { seen[base] += 1; return `${base}_${seen[base]}`; }
        seen[base] = 1;
        return base;
      });

      const tbody = table.querySelector("tbody");
      if (!tbody) return { headers, players: [] };

      const rows = Array.from(tbody.querySelectorAll("tr[role='row'], tr"));
      const players = [];

      for (const row of rows) {
        const tds = Array.from(row.querySelectorAll("td"));
        if (!tds.length) continue;

        const nameText = tds[1] ? cellValue(tds[1]) : "";
        if (nameText === "TEAM" || nameText === "Totals" || nameText === "Opponent Totals") continue;

        const entry = {};
        for (let i = 0; i < headers.length; i++) {
          const h = headers[i];
          const td = tds[i];
          if (!td) { entry[h] = ""; continue; }
          const value = cellValue(td);

          let link = null;
          let pid = null;
          const a = td.querySelector("a[href]");
          if (a) {
            link = a.getAttribute("href");
            const m = (link || "").match(/\/players\/(\d+)/);
            if (m) pid = m[1];
          }

          if (h.toLowerCase().startsWith("player") && pid) {
            entry[h] = { text: value, link, player_id: pid };
          } else {
            entry[h] = value;
          }
        }
        players.push(entry);
      }

      return { headers, players };
    }
    """
    data = await page.evaluate(js)
    return data.get("headers", []), data.get("players", [])


# -------------------------
# Scraping (single team)
# -------------------------
async def scrape_team_with_proxy(
    playwright,
    team: Dict[str, str],
    sport: str,
    proxy_line: Optional[str],
    per_category_delay: Tuple[float, float],
) -> Dict[str, Any]:
    tid = str(team["id"])
    base_url = f"https://stats.ncaa.org/teams/{tid}/season_to_date_stats"
    is_football = "football" in sport.lower()

    proxy_cfg = parse_proxy(proxy_line) if proxy_line else None

    browser = await playwright.chromium.launch(
        headless=True,  # TEMP: diagnostic
        proxy=proxy_cfg,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )


    context = await browser.new_context(
        viewport=random.choice(VIEWPORTS),
        user_agent=random.choice(USER_AGENTS),
    )

    page = await context.new_page()
    await page.set_extra_http_headers(random_headers())

    try:
        await page.goto(base_url, wait_until="domcontentloaded", timeout=180_000)

        title = await page.title()
        html = await page.content()

        if detect_block(title) or detect_block(html[:9000]):
            print("DEBUG url:", page.url)
            print("DEBUG title:", title)
            print("DEBUG html head:", html[:600].replace("\n", " ")[:600])
            raise RuntimeError("BLOCKED_ACCESS_DENIED")

        # ensure table exists
        try:
            await page.wait_for_selector("table#stat_grid", timeout=45_000)
        except PlaywrightTimeoutError:
            await page.mouse.wheel(0, 900)
            await asyncio.sleep(random.uniform(1.5, 3.0))

        # ---------- football: multiple categories ----------
        if is_football:
            cats = await page.evaluate(
                r"""
                () => {
                  const categories = [];
                  const navTabs = document.querySelectorAll('ul.nav.nav-tabs li.nav-item a.nav-link');
                  for (const tab of navTabs) {
                    const name = (tab.textContent || '').trim();
                    const href = tab.getAttribute('href') || '';
                    const match = href.match(/year_stat_category_id=(\d+)/);
                    if (match && name) {
                      categories.push({ name, category_id: match[1], url: href });
                    }
                  }
                  return categories;
                }
                """
            )
            if not cats:
                raise RuntimeError("NO_FOOTBALL_CATEGORIES")

            all_categories: Dict[str, Any] = {}

            for cat in cats:
                await asyncio.sleep(random.uniform(*per_category_delay))

                href = cat["url"]
                full_url = href if not href.startswith("/") else f"https://stats.ncaa.org{href}"

                await page.set_extra_http_headers(random_headers())
                await page.goto(full_url, wait_until="domcontentloaded", timeout=180_000)

                try:
                    await page.wait_for_selector(
                        "div.dataTables_scrollBody table#stat_grid tbody tr",
                        timeout=60_000,
                    )
                except PlaywrightTimeoutError:
                    html2 = await page.content()
                    if detect_block(html2[:9000]):
                        raise RuntimeError("BLOCKED_ACCESS_DENIED")
                    continue

                headers, players = await extract_player_table_from_dom(page)
                if players:
                    all_categories[cat["name"]] = {
                        "category_id": cat["category_id"],
                        "url": full_url,
                        "headers": headers,
                        "players": players,
                    }

            if not all_categories:
                raise RuntimeError("NO_FOOTBALL_DATA_EXTRACTED")

            return {
                "team_id": tid,
                "team_name": team["name"],
                "team_url": base_url,
                "stat_categories": all_categories,
            }

        # ---------- non-football ----------
        await page.wait_for_selector(
            "div.dataTables_scrollBody table#stat_grid tbody tr",
            timeout=60_000,
        )

        headers, players = await extract_player_table_from_dom(page)
        if not players:
            raise RuntimeError("NO_PLAYERS_EXTRACTED")

        return {
            "team_id": tid,
            "team_name": team["name"],
            "team_url": base_url,
            "headers": headers,
            "players": players,
        }

    finally:
        await context.close()
        await browser.close()


# -------------------------
# Main
# -------------------------
async def main_async(args):
    division_filter = None if args.all_divisions else (args.division.upper() if args.division else None)

    team_list_path = f"organized_by_division_conference/refined_team_lists/{args.sport}_teams_d1-3_list_by_conference_division.json"
    if not Path(team_list_path).exists():
        raise FileNotFoundError(f"Team list file not found: {team_list_path}")

    teams = load_team_list(team_list_path, division_filter)
    print(f"[+] Loaded {len(teams)} teams")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    done_ids = set()

    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text())
            for item in existing:
                results.append(item)
                done_ids.add(str(item["team_id"]))
            print(f"[+] Resuming: {len(results)} teams already saved")
        except Exception as e:
            print(f"[!] Could not resume: {e}")

    remaining = [t for t in teams if str(t["id"]) not in done_ids]
    print(f"[+] Remaining: {len(remaining)} teams")

    proxies = load_proxies(args.proxy_file)
    proxy_rotator = ProxyRotator(proxies)

    print(f"[+] Loaded {len(proxies)} proxies (no HTTPX health check)")

    per_team_delay = (args.min_gap, args.max_gap)
    per_category_delay = (args.min_cat_delay, args.max_cat_delay)

    async with async_playwright() as playwright:
        for i, team in enumerate(remaining, 1):
            tid = str(team["id"])
            print(f"\n[{i}/{len(remaining)}] {team['name']} ({tid})")

            gap = random.uniform(*per_team_delay)
            print(f"    [gap] sleep {gap:.1f}s")
            await asyncio.sleep(gap)

            success = False
            last_err: Optional[Exception] = None

            for attempt in range(1, args.team_retries + 1):
                proxy_line = proxy_rotator.next()

                try:
                    if proxy_line:
                        # show only server (avoid printing creds)
                        try:
                            pcfg = parse_proxy(proxy_line)
                            print(f"    [proxy] {pcfg.get('server')}")
                        except Exception:
                            print("    [proxy] (invalid proxy line)")
                    else:
                        print("    [proxy] NONE")

                    data = await scrape_team_with_proxy(
                        playwright=playwright,
                        team=team,
                        sport=args.sport,
                        proxy_line=proxy_line,
                        per_category_delay=per_category_delay,
                    )

                    results.append(data)
                    done_ids.add(tid)
                    out_path.write_text(json.dumps(results, indent=2))
                    print(f"    [save] {args.output} ({len(results)} teams)")
                    success = True
                    break

                except Exception as e:
                    last_err = e
                    msg = str(e)
                    print(f"    [FAIL] attempt {attempt}/{args.team_retries}: {e}")

                    # backoff for transient failures
                    backoff = (2 ** (attempt - 1)) * args.base_backoff + random.uniform(1, 4)
                    print(f"    [backoff] sleep {backoff:.1f}s")
                    await asyncio.sleep(backoff)

            if not success:
                print(f"[!] Giving up on team {team['name']} ({tid}). Last error: {last_err}")

    print(f"\n[✓] Done. Saved {len(results)} teams → {args.output}")


def main():
    p = argparse.ArgumentParser("NCAA scraper (Playwright, proxy-per-team, no HTTPX test)")

    p.add_argument("sport", help="football, mens_basketball, baseball, etc")
    p.add_argument("division", nargs="?", help="D1, D2, D3 (optional)")
    p.add_argument("--all-divisions", action="store_true")

    p.add_argument("--output", default="sport_data/output.json")
    p.add_argument("--proxy-file", default=None, help="proxies.txt (1 per line)")

    # pacing
    p.add_argument("--min-gap", type=float, default=25.0)
    p.add_argument("--max-gap", type=float, default=60.0)

    p.add_argument("--min-cat-delay", type=float, default=3.0)
    p.add_argument("--max-cat-delay", type=float, default=7.0)

    # retries
    p.add_argument("--team-retries", type=int, default=3)
    p.add_argument("--base-backoff", type=float, default=8.0)

    args = p.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
