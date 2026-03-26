# GenizahSearch Analytics Report
**Period: Feb 27 - Mar 13, 2026 (14 days)**
**Generated: 2026-03-13 from PostHog**

---

## 1. Overall Traffic Stats

| Metric | 14-Day Total |
|--------|-------------|
| Total Pageviews | **1,676** |
| Unique Sessions | **453** |
| Peak DAU | **35** (Mar 10) |
| Avg DAU (active days) | ~21 |
| Total Searches | **1,099** |
| Parallels Searches | **90** |
| Results Opened (expanded) | **50** |
| Rage Clicks | **557** (see Section 8 — mostly false positives) |
| Successful Logins | **4** |
| Failed Logins | **7** (see Section 9) |

### Daily Breakdown

| Date | Users | Events | Searched | Rage Clicks |
|------|-------|--------|----------|-------------|
| Mar 1 | 8 | 83 | 0 | 1 |
| Mar 2 | 34 | 1,388 | 0 | 24 |
| Mar 3 | 31 | 1,640 | 0 | 18 |
| Mar 4 | 32 | 1,417 | 0 | 11 |
| Mar 5 | 35 | 2,078 | 12 | 31 |
| Mar 6 | 12 | 694 | 6 | 8 |
| Mar 7 | 12 | 430 | 6 | 3 |
| Mar 8 | 30 | 1,135 | 16 | 11 |
| Mar 9 | **43** | **2,280** | 26 | 31 |
| Mar 10 | **44** | 1,928 | 24 | 25 |
| Mar 11 | 32 | 2,784 | 16 | 153 |
| Mar 12 | 28 | 2,275 | 11 | 234 |
| Mar 13 | 22 | 546 | 10 | 7 |

**Pattern:** Weekday traffic (Sun-Thu Israel time) peaks at 30-44 DAU. Fri/Sat drops to 8-12. Search tracking started Mar 5.

---

## 2. Page Popularity (combined www + non-www)

| Page | Pageviews | % of Total |
|------|-----------|------------|
| `/search` | **424** | 25.3% |
| `/` (homepage) | **310** | 18.5% |
| `/parallels` | **116** | 6.9% |
| `/browse` | **60** | 3.6% |
| `/about` | **34** | 2.0% |
| `/download` | **23** | 1.4% |
| `/catalog-browse` | **17** | 1.0% |
| Other (search results, admin, etc.) | **642** | 38.3% |

Search is the #1 feature — over 25% of all pageviews. Parallels is the 3rd most visited feature page.

---

## 3. User Navigation Paths (Top Flows)

| From | To | Users | Avg Time |
|------|----|-------|----------|
| Homepage | `/search` | **46** | 2.5 min |
| Homepage | `/parallels` | **16** | 1.5 min |
| Homepage | `/download` | **9** | 12 sec |
| Homepage | `/browse` | **4** | 1.5 min |
| Homepage | `/about` | **3** | 14 sec |
| Homepage | `/catalog-browse` | **3** | 44 sec |
| `/search` | Homepage (back) | **11** | 8 min |
| `/search` (www) | `/browse` | **5** | 4.6 min |
| Facebook landing | `/search` | **6** | 32 sec |

Dominant flow: Homepage → Search. Users spend ~8 min on search before navigating elsewhere. Facebook visitors navigate to search within 30 seconds.

---

## 4. Traffic Sources (Referrers)

| Source | Pageviews | % |
|--------|-----------|---|
| **Direct** (bookmarks/typed) | **952** | 56.8% |
| **Internal** (genizahsearch.com) | **393** | 23.5% |
| **Google** | **178** | 10.6% |
| **Facebook** | **39** | 2.3% |
| **Otzar HaChochma Forum** | **27** | 1.6% |
| **GatekeeperApp** | **26** | 1.6% |
| **Twitter/X** (t.co) | **24** | 1.4% |
| **Gmail** (Android) | **18** | 1.1% |
| **GitHub** | **10** | 0.6% |

Over half the traffic is direct — users are bookmarking/returning. Google organic is strong at ~11%. The Otzar HaChochma forum referral is notable — an academic/rabbinic community is linking to GenizahSearch.

---

## 5. Device & Browser Breakdown

### Devices
| Device | Pageviews | % |
|--------|-----------|---|
| Desktop | **1,548** | 92.4% |
| Mobile | **122** | 7.3% |
| Tablet | **6** | 0.4% |

### Browsers
| Browser | Pageviews | % |
|---------|-----------|---|
| Chrome | **1,488** | 88.8% |
| Safari | **77** | 4.6% |
| Firefox | **72** | 4.3% |
| Edge | **27** | 1.6% |
| Opera | **6** | 0.4% |
| Mobile Safari | **3** | 0.2% |

Overwhelmingly desktop + Chrome. Typical for an academic research tool.

---

## 6. Geographic Distribution

| Country | Pageviews | % |
|---------|-----------|---|
| **Israel** | **1,083** | 64.6% |
| **USA** | **217** | 12.9% |
| **UK** | **216** | 12.9% |
| **Germany** | **96** | 5.7% |
| **France** | **46** | 2.7% |
| **Panama** | **9** | 0.5% |
| Australia, New Zealand, China, Ireland, Mexico | 1-3 each |

Israel dominates (65%), with US and UK tied for 2nd (~13% each). Matches expected Genizah research community (Hebrew U, Cambridge, JTS).

---

## 7. Top 20 Power Users

| # | User ID (last 6) | Total Events | Searches | Results Opened | Pageviews |
|---|-------------------|-------------|----------|----------------|-----------|
| 1 | `ccadfe` | **2,853** | 0 | 0 | 67 |
| 2 | `8f2456` | **1,958** | 201 | 1 | 116 |
| 3 | `e8cd42` | **1,258** | 203 | 17 | 47 |
| 4 | `87efcf` | **599** | 48 | 0 | 54 |
| 5 | `69418a` | **574** | 6 | 0 | 54 |
| 6 | `191dbf` | **522** | 0 | 0 | 94 |
| 7 | `256693` | **463** | 40 | 0 | 43 |
| 8 | `9dc473` | **401** | 49 | 14 | 28 |
| 9 | `ce8283` | **351** | 55 | 0 | 39 |
| 10 | `6b228d` | **319** | 12 | 0 | 27 |

### Top 5 User Profiles

| User | Sessions | Top Pages | Profile |
|------|----------|-----------|---------|
| `ccadfe` | 26 | Home, Search, **Parallels** | Heavy parallels user, fast-clicker (triggers false rage clicks) |
| `8f2456` | 44 | Search (www), Browse | Power searcher — 201 searches, Hebrew queries like "תקאטע" |
| `e8cd42` | 48 | Search, Browse, Home | Most engaged researcher — 203 searches, **17 results opened** (highest), daily user |
| `87efcf` | 32 | Search, Home | Regular searcher — Hebrew queries like "ובהזכירם את השם המפורש" |
| `69418a` | 5 | Home, **Parallels**, Download | Parallels specialist — deep engagement in few sessions |

---

## 8. Rage Clicks — FALSE POSITIVES (Investigated)

**344 of 360 parallels rage clicks = ONE user** (`ccadfe`).

Analyzed their click stream: pattern is 3 rapid autocapture events (~150ms apart) triggering PostHog's rage click threshold, followed by calm continued usage. The user doesn't leave — they keep working for hours across 26 sessions. This is a **fast double/triple-click habit** (likely text selection or list interaction), not UX frustration.

Other 9 users with parallels rage clicks had 1-3 each — negligible.

**Verdict: No action needed.** Consider filtering this user from rage click metrics or raising the detection threshold.

---

## 9. Login Failures — Investigation

### Issue 1: `supabase_url is required` (4 failures on Mar 9) — FIXED

Four users on Mar 9 got "Login error: supabase_url is required" — the Supabase URL env var wasn't loaded during their login attempt. **This has been fixed** (environment loading made reliable).

### Issue 2: `Invalid login credentials` (3 failures from 1 user on Mar 12)

One user from Pikesville, Maryland (Hebrew-speaking, arrived via Google) attempted login 3 times in 16 seconds on the `/browse` page — all with wrong credentials. Not a bug — user likely forgot password or doesn't have an account.

**Recommendation:** Make the "Forgot password?" link more prominent on the login dialog.

### Successful Logins (4 total — all worked)

| When | User | Where |
|------|------|-------|
| Mar 10, 05:52 | `6901b7...` | Munich, Germany |
| Mar 10, 09:03 | `bafb4b...` | Tel Aviv |
| Mar 10, 11:30 | adiel.breuer@mail.huji.ac.il | Tel Aviv (magic link signup) |
| Mar 11, 10:42 | `b93c7c...` | Tel Aviv (mobile) |

---

## 10. Errors & Application Health

- **PostHog Error Tracking: 0 active errors** — No tracked application errors in the period.
- Application is stable.

---

## 11. Key Findings & Recommendations

### What's Working Well
1. **Strong organic growth** — 178 pageviews from Google, growing daily
2. **High return rate** — 57% direct traffic = loyal returning users
3. **Search is the killer feature** — 1,099 searches in 9 days of tracking
4. **Community awareness** — referrals from Otzar forum, Twitter, Facebook, Gmail
5. **Zero tracked errors** — stable application
6. **Power users are deeply engaged** — top user has 48 sessions in 14 days

### Recommendations
1. **Improve "Forgot password?" visibility** — one user failed 3x rapidly
2. **Low result_opened rate (4.5%)** — investigate if expanding results is discoverable enough, or if compact view suffices
3. **Consider mobile UX audit** — only 7% traffic, may be fine for academic audience
4. **Monitor Otzar forum + social referrals** — community growth channel
