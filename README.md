# Negative Keyword Analysis: Word-Level Post-Click Behavior

**Identify wasted paid search spend using post-click behavioral data — at scale.**

Most paid search optimization stops at the click. This methodology starts there.

By breaking search queries into individual words and aggregating GA4 post-click behavior (bounce rate, sessions, cost) at the word level, this approach surfaces unqualified traffic patterns that traditional query-level review consistently misses.

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

These patterns repeat across industries. Every large paid search account has them. The question is whether you have a method to find them.

---

## Implementations

This repo contains three implementations of the same methodology, suited to different technical environments:

### Option A: SQL / BigQuery (Automated, Always Current)

**[`negative_keyword_analysis.sql`](negative_keyword_analysis.sql)**

- Requires: Google Ads → BigQuery Data Transfer + GA4 → BigQuery export
- Runs automatically, always reflects the latest 90 days of data
- No manual maintenance required
- Connect the resulting BigQuery view directly to Looker Studio

Best for: Organizations with data engineering support or existing BigQuery pipelines.

### Option B: Python (Semi-Automated, No BigQuery Required)

**[`negative_keyword_analysis.py`](negative_keyword_analysis.py)**

- Requires: GA4 with Google Ads connected, Python, pandas, NLTK
- Export a custom GA4 Explore report to CSV, run the script
- Includes an `investigate_word()` helper to drill into any flagged word
- Sample dataset included — run it immediately with no setup

Best for: Data scientists and analysts comfortable with Python who don't have BigQuery access.

### Option C: R (Alternative to Python)

**[`negative_keyword_analysis.R`](negative_keyword_analysis.R)**

- Requires: R, googleAnalyticsR, qdap, stopwords packages
- Pulls data directly from Google Analytics API (Universal Analytics / GA3)
- Includes more sophisticated preprocessing: geographic term removal, brand detection, Spanish stopwords
- Note: Written for Universal Analytics. For GA4, use the Python or SQL version.

Best for: R users or organizations still on Universal Analytics.

---

## Getting Started (Python — Quickest Path)

```bash
# Install dependencies
pip install pandas nltk

# Run with included sample data
python negative_keyword_analysis.py
```

The script will produce two output files:
- `high_bounce_words.csv` — words flagged for high bounce rate
- `flagged_queries.csv` — all queries containing flagged words

To use your own data, export a GA4 Explore report and update `DATA_PATH` in the script.

---

## Looker Studio Dashboard

A live Looker Studio dashboard is available that connects to the BigQuery view produced by the SQL implementation.

🔗 **[View Demo Dashboard](#)** *(link coming soon)*

The dashboard shows:
- Word-level bounce rate and cost ranked by spend at risk
- Query drill-down for any flagged word
- Pre/post implementation trend view

To use with your own data: open the dashboard, click **Make a copy**, and connect to your own BigQuery view or Google Sheet.

---

## Data Requirements

### For the SQL / BigQuery version
You need two data pipelines flowing into BigQuery:
1. **Google Ads → BigQuery** via [Google Ads BigQuery Data Transfer Service](https://cloud.google.com/bigquery-transfer/docs/google-ads-transfer)
2. **GA4 → BigQuery** via [native GA4 BigQuery export](https://support.google.com/analytics/answer/9823238)

See the comments in `negative_keyword_analysis.sql` for the expected schema.

### For the Python / R versions
You need:
- GA4 with Google Ads linked (so query and cost data flow into GA sessions)
- A custom GA4 Explore report exported as CSV with:
  - Dimension: Session Google Ads query
  - Metrics: Sessions, Engaged sessions, Google Ads cost

---

## Customizing for Your Industry

### Protect multi-word terms
If your industry has meaningful multi-word phrases (disease names, brand names, product categories), protect them from being split during tokenization.

In SQL:
```sql
REPLACE(session_google_ads_query, 'your multi word term', 'your_multi_word_term')
```

In Python:
```python
PROTECTED_PHRASES = {
    'your multi word term': 'your_multi_word_term'
}
```

### Extend the stopword list
The default stopword list covers common English stopwords. Add industry-specific terms that appear frequently but carry no intent signal — product category words, generic descriptors, etc.

### Adjust thresholds
Start with `BOUNCE_RATE_THRESHOLD = 0.50` and `MIN_SESSIONS = 3`. Adjust based on your account's traffic volume and typical bounce rates.

---

## Article

This methodology is described in detail in:

**[Finding Hidden Wasted Spend in Paid Search Using Post-Click Behavior](#)** *(link coming soon)*

Published on Toward Data Science.

---

## Author

**Elizabeth Lawson**
Director of Data Science | Marketing Analytics

[Website](https://www.elizabeth-lawson-analytics.com) · [LinkedIn](https://linkedin.com/in/elizabethalawson) · [Case Study](https://www.elizabeth-lawson-analytics.com/paid-search-optimization-case-study)

---

## License

MIT License — free to use, adapt, and share with attribution.
