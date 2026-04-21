# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# Python — Manual Export Version
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Identifies potential negative keywords by breaking paid search
#   queries into individual words and aggregating GA4 post-click
#   behavioral metrics (bounce rate, sessions, cost) at the word level.
#
#   This version uses a CSV exported from Looker Studio or GA4 Explore.
#   No API access or BigQuery required.
#
# Requirements:
#   pip install pandas
#
# Data Setup:
#   Option A — Export from Looker Studio:
#     1. Open your Looker Studio report connected to GA4
#     2. Add a table with:
#          Dimension: Session Google Ads query
#          Metrics:   Sessions, Engaged sessions, Ads cost
#     3. Click the three dots on the table → Export → CSV
#     4. Rename the file to sample_data.csv or update DATA_PATH below
#
#   Option B — Export from GA4 Explore:
#     1. Create a Free Form exploration in GA4 with:
#          Dimension: Session Google Ads query
#          Metrics:   Sessions, Engaged sessions, Google Ads cost
#     2. Export as CSV (requires Editor access on the GA4 property)
#     3. Set SKIP_ROWS = 6 for raw GA4 exports
#     4. Set COST_COLUMN = 'Google Ads cost'
#
#   The included sample_data.csv uses the Google Merchandise Store
#   demo account so you can run the analysis immediately.
#
# Output:
#   word_level_analysis.csv — all words with sessions, bounces, bounce_rate, cost

#
#   Both files use standardized column names matching the BigQuery
#   SQL version so all implementations connect to the same
#   Looker Studio dashboard without any field remapping.
#
# Usage:
#   1. Update DATA_PATH to point to your exported CSV
#   2. Update BRAND_TERMS with your brand name(s)
#   3. Adjust thresholds as needed
#   4. Run the script
#   4. Load word_level_analysis.csv into Google Sheets

#
# For the GA4 Data API version (no manual export required) see:
#   negative_keyword_analysis_api.py
# ============================================================

import pandas as pd
import re

# ============================================================
# Configuration
# ============================================================

DATA_PATH = 'sample_data.csv'

# Column names in your input file
# Looker Studio export: COST_COLUMN = 'Ads cost'
# GA4 Explore export:   COST_COLUMN = 'Google Ads cost'
QUERY_COLUMN    = 'Session Google Ads query'
SESSIONS_COLUMN = 'Sessions'
ENGAGED_COLUMN  = 'Engaged sessions'
COST_COLUMN     = 'Ads cost'

# Number of header rows to skip
# Looker Studio exports: 0
# Raw GA4 Explore exports: 6
SKIP_ROWS = 0

# ============================================================
# Brand terms
# Replace with your own brand name(s) and all variations.
# These are removed before analysis so branded terms do not
# distort the word-level results.
# ============================================================
BRAND_TERMS = {
    'your_brand_name',          # Replace with your actual brand name
    # 'your_brand_abbreviation',
    # 'common_misspelling',
}

# ============================================================
# Protected phrases
# Multi-word terms that should be treated as a single token.
# Format: {'original phrase': 'replacement_with_underscores'}
# ============================================================
PROTECTED_PHRASES = {
    # Healthcare examples:
    # 'myasthenia gravis': 'myasthenia_gravis',
    # 'lambert eaton':     'lambert_eaton',
    # Automotive examples:
    # 'rolls royce':       'rolls_royce',
    # 'alfa romeo':        'alfa_romeo',
}

# Thresholds
MIN_SESSIONS = 3
OUTPUT_PATH  = 'word_level_analysis.csv'


# ============================================================
# Stopwords
# ============================================================

ENGLISH_STOPWORDS = {
    'i','me','my','myself','we','our','ours','ourselves',
    'you','your','yours','yourself','yourselves',
    'he','him','his','himself','she','her','hers','herself',
    'it','its','itself','they','them','their','theirs','themselves',
    'what','which','who','whom','this','that','these','those',
    'am','is','are','was','were','be','been','being',
    'have','has','had','having','do','does','did','doing',
    'a','an','the','and','but','if','or','because','as','until',
    'while','of','at','by','for','with','about','against',
    'between','into','through','during','before','after',
    'above','below','to','from','up','down','in','out',
    'on','off','over','under','again','further','then','once',
    'here','there','when','where','why','how',
    'all','any','both','each','few','more','most','other',
    'some','such','no','nor','not','only','own','same',
    'so','than','too','very','s','t',
    'can','will','just','don','should','now',
    'store','stores','near','me','shop','buy','online','get',
    'best','cheap','affordable','new','used'
}

SPANISH_STOPWORDS = {
    'de','la','el','en','y','a','los','las','un','una','es',
    'por','con','no','su','para','como','pero','sus','le',
    'ya','o','porque','cuando','muy','sin','sobre','también',
    'hasta','hay','donde','quien','desde','todo','nos',
    'durante','eso','mi','del','se','lo','da','si','al','e',
    'cerca','tienda'
}

US_STATES = {
    'alabama','alaska','arizona','arkansas','california','colorado',
    'connecticut','delaware','florida','georgia','hawaii','idaho',
    'illinois','indiana','iowa','kansas','kentucky','louisiana',
    'maine','maryland','massachusetts','michigan','minnesota',
    'mississippi','missouri','montana','nebraska','nevada',
    'hampshire','jersey','mexico','york','carolina','dakota',
    'ohio','oklahoma','oregon','pennsylvania','rhode','island',
    'tennessee','texas','utah','vermont','virginia','washington',
    'wisconsin','wyoming',
    'al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id',
    'il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms',
    'mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok',
    'or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv',
    'wi','wy','dc'
}

ALL_STOPWORDS = ENGLISH_STOPWORDS | SPANISH_STOPWORDS | US_STATES


# ============================================================
# Load and prepare data
# ============================================================

df = pd.read_csv(DATA_PATH, skiprows=SKIP_ROWS)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df = df.dropna(subset=[QUERY_COLUMN])

for phrase, replacement in PROTECTED_PHRASES.items():
    df[QUERY_COLUMN] = df[QUERY_COLUMN].str.replace(
        phrase, replacement, case=False, regex=False
    )

df['bounces']     = df[SESSIONS_COLUMN] - df[ENGAGED_COLUMN]
df['bounce_rate'] = df['bounces'] / df[SESSIONS_COLUMN]

print(f"Loaded {len(df):,} queries")


# ============================================================
# Tokenize
# ============================================================

long_data_rows = []

for _, row in df.iterrows():
    query = str(row[QUERY_COLUMN])
    words = [
        w.lower().strip('.,!?()[]"\'-')
        for w in query.split()
    ]
    words = [
        w for w in words
        if w not in ALL_STOPWORDS
        and w not in BRAND_TERMS
        and not re.search(r'\d', w)
        and len(w) > 1
    ]
    for word in words:
        long_data_rows.append({
            'session_google_ads_query': str(row[QUERY_COLUMN]),
            'word':                     word,
            'advertiser_ad_cost':       row.get(COST_COLUMN, 0),
            'sessions':                 row[SESSIONS_COLUMN],
            'bounces':                  row['bounces'],
            'bounce_rate':              row['bounce_rate']
        })

long_data = pd.DataFrame(long_data_rows)
long_data['sessions'] = pd.to_numeric(
    long_data['sessions'], errors='coerce'
).fillna(0).astype(int)
long_data = long_data[long_data['sessions'] > 0]

print(f"Tokenized into {len(long_data):,} word-level rows")


# ============================================================
# Aggregate to word level
# ============================================================

# ============================================================
# Save output
# One row per word per query — Looker Studio handles aggregation.
# Load this into Google Sheets and connect Looker Studio to it.
# ============================================================

output = long_data[[
    'word',
    'session_google_ads_query',
    'sessions',
    'bounces',
    'bounce_rate',
    'advertiser_ad_cost'
]].copy()

output = output.sort_values(
    by=['word', 'sessions'], ascending=[True, False]
)

output.to_csv(OUTPUT_PATH, index=False)
print(f"
Saved {len(output):,} rows to: {OUTPUT_PATH}")
print("Load this file into Google Sheets to connect to your Looker Studio dashboard.")
