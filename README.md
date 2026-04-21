# Negative Keyword Analysis: Word-Level Post-Click Behavior

**Identify wasted paid search spend using post-click behavioral data — at scale.**

Most paid search optimization stops at the click. This methodology starts there.

By breaking search queries into individual words and aggregating GA4 post-click behavior (bounce rate, sessions, cost) at the word level, this approach surfaces unqualified traffic patterns that traditional query-level review consistently misses.

**See full case study:** [https://www.elizabeth-lawson-analytics.com/paid-search-optimization-case-study]

**View demo dashboard:** [https://datastudio.google.com/reporting/f2c86a83-417b-4f37-b9b2-7483973cf5ed]

---

## The Problem

Standard negative keyword workflows rely on:
- Manual review of search term reports
- Predefined exclusion lists
- Surface-level query relevance

This leaves two critical gaps: no connection to post-click behavior, and no scalable way to identify unexpected or ambiguous intent. The result is campaigns that continue spending against traffic with no realistic path to conversion.

## The Insight

Individual words that signal mismatched intent are often invisible when buried inside longer queries — but they surface clearly when aggregated across thousands of sessions.

**Real examples of what this methodology finds:**

| Word | Industry | Why It's Unqualified |
|------|----------|----------------------|
| Lamborghini | Healthcare / Rare Disease | Users searching for Lambert-Eaton syndrome, not the advertised drug |
| Magnesium | Healthcare | Users searching for supplements; campaign targets MG (Myasthenia Gravis) |
| Craigslist | Automotive | Users searching for peer-to-peer listings, not dealership inventory |
| Portal | Automotive | Users trying to access payment/employee systems |
| Vyepti | Healthcare | Competitor treatment, not the advertised condition |
| Piano | Furniture / Retail | Users searching for piano-shaped furniture, not general furniture buyers |
| Schewels | Furniture / Retail | Regional competitor — users looking for a specific other store |

These patterns repeat across industries. Every large paid search account has them. The question is whether you have a method to find them.

---

## Implementations

This repo contains five implementations of the same methodology. All produce identical output — choose the one that fits your environment.

### Option A: SQL / BigQuery (Fully Automated)

**[`negative_keyword_analysis.sql`](negative_keyword_analysis.sql)**

- Requires: Google Ads → BigQuery via Data Transfer Service + GA4 → BigQuery export
- Runs automatically, always reflects the latest data
- No manual maintenance required
- Connect the resulting BigQuery view directly to Looker Studio

Best for: Organizations with data engineering support or existing BigQuery pipelines.

---

### Option B: Manual Export (No Infrastructure Required)

Export your data from Looker Studio or GA4 Explore, then run either script below.
The sample dataset (`sample_data.csv`) uses the Google Merchandise Store demo account
so you can run the analysis immediately without your own data.

**Python:** [`negative_keyword_analysis_manual.py`](negative_keyword_analysis_manual.py)
```bash
pip install pandas
python negative_keyword_analysis_manual.py
```

**R:** [`negative_keyword_analysis_manual.R`](negative_keyword_analysis_manual.R)
```r
install.packages("readr")
source("negative_keyword_analysis_manual.R")
```

Best for: Anyone comfortable with Python or R who does not have BigQuery access.

**Data export steps:**

*From Looker Studio:*
1. Add a table to your report with: Session Google Ads query, Sessions, Engaged sessions, Ads cost
2. Click the three dots on the table → Export → CSV
3. Rename the file or update `DATA_PATH` in the script

*From GA4 Explore:*
1. Create a Free Form exploration with: Session Google Ads query, Sessions, Engaged sessions, Google Ads cost
2. Export as CSV (requires Editor access)
3. Set `SKIP_ROWS = 6` and `COST_COLUMN = 'Google Ads cost'` in the script

---

### Option C: GA4 Data API (Automated, No BigQuery Required)

Pull data directly from GA4 without any manual export.
Requires a Google Cloud service account with GA4 Data API access.

**Python:** [`negative_keyword_analysis_api.py`](negative_keyword_analysis_api.py)
```bash
pip install pandas google-analytics-data
python negative_keyword_analysis_api.py
```

**R:** [`negative_keyword_analysis_api.R`](negative_keyword_analysis_api.R)
```r
install.packages("googleAnalyticsR")
source("negative_keyword_analysis_api.R")
```

Best for: Teams that want automation without setting up BigQuery pipelines.

**Setup:**
1. Create a service account in [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Enable the [Google Analytics Data API](https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com)
3. Download the JSON key file
4. Grant the service account Viewer access to your GA4 property
5. Update `KEY_FILE_PATH` and `GA4_PROPERTY_ID` in the script

---

## All Scripts Produce the Same Output

Regardless of which implementation you use, the output is identical:

- **`high_bounce_words.csv`** — words flagged for high bounce rate, sorted by bounce rate
- **`flagged_queries.csv`** — all queries containing flagged words for manual review

Load `high_bounce_words.csv` into Google Sheets and connect your Looker Studio dashboard to that sheet.

---

## Looker Studio Dashboard

A live demo dashboard is available connected to the Google Merchandise Store demo data.

🔗 **[View Demo Dashboard](#)** *(link coming soon)*

The dashboard shows:
- Word-level bounce rate and cost, ranked by spend at risk
- Query drill-down for any flagged word
- Trend view before and after implementation

To use with your own data: open the dashboard → **Make a copy** → connect to your own BigQuery view or Google Sheet.

---

## Customizing for Your Use Case

### Brand terms
Add your brand name(s) to `BRAND_TERMS` so branded queries don't distort results:
```python
BRAND_TERMS = {'your_brand_name', 'brand_abbreviation'}
```

### Protected phrases
Preserve multi-word terms that should not be split during tokenization:
```python
PROTECTED_PHRASES = {
    'myasthenia gravis': 'myasthenia_gravis',
    'rolls royce': 'rolls_royce',
}
```

### Stopwords
All implementations include English, Spanish, and US geographic stopwords by default.
Add industry-specific terms that carry no intent signal for your campaigns.

### Thresholds
Start with `BOUNCE_RATE_THRESHOLD = 0.50` and `MIN_SESSIONS = 3`.
Increase `MIN_SESSIONS` for high-traffic accounts to reduce noise.

---

## Article

This methodology is described in detail in:

**[Finding Hidden Wasted Spend in Paid Search Using Post-Click Behavior](#)** *(link coming soon)*

Published on Toward Data Science.

---

## Author

**Elizabeth Lawson**
Data Science Leader | Marketing Analytics

[Website](https://www.elizabeth-lawson-analytics.com) · [LinkedIn](https://linkedin.com/in/elizabethalawson) · [Case Study](https://www.elizabeth-lawson-analytics.com/paid-search-optimization-case-study) · [GitHub](https://github.com/elizabethlawson-analytics)

---

## License

MIT License — free to use, adapt, and share with attribution.
