# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# Python — GA4 Data API Version (No Manual Export Required)
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Pulls data directly from the GA4 Data API and identifies
#   potential negative keywords using word-level post-click
#   behavioral analysis.
#
# Requirements:
#   pip install pandas google-analytics-data
#
# Setup:
#   1. Create a Google Cloud project
#      https://console.cloud.google.com
#   2. Enable the Google Analytics Data API
#      https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
#   3. Create a service account and download the JSON key
#      https://console.cloud.google.com/iam-admin/serviceaccounts
#   4. Grant the service account Viewer access to your GA4 property
#      In GA4: Admin → Account Access Management → Add users
#   5. Update KEY_FILE_PATH and GA4_PROPERTY_ID below
#
# Output:
#   word_level_analysis.csv — all words with sessions, bounces, bounce_rate, cost

#
#   Both files use standardized column names matching the BigQuery
#   SQL version so all implementations connect to the same
#   Looker Studio dashboard without any field remapping.
#
# Note:
#   The GA4 Data API does not support the Google Analytics demo account.
#   Use negative_keyword_analysis_manual.py with sample_data.csv
#   to test with demo data.
# ============================================================

import pandas as pd
import re
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest,
    FilterExpression, Filter,
)
from google.oauth2 import service_account

# ============================================================
# Configuration
# ============================================================

KEY_FILE_PATH   = 'your-service-account-key.json'
GA4_PROPERTY_ID = 'your-ga4-property-id'
START_DATE      = '90daysAgo'
END_DATE        = 'today'
MAX_ROWS        = 10000

# ============================================================
# Brand terms
# ============================================================
BRAND_TERMS = {
    'your_brand_name',
    # 'your_brand_abbreviation',
}

# ============================================================
# Protected phrases
# ============================================================
PROTECTED_PHRASES = {
    # 'myasthenia gravis': 'myasthenia_gravis',
    # 'rolls royce': 'rolls_royce',
}

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
# Pull data from GA4 Data API
# ============================================================

def pull_ga4_data():
    print(f"Connecting to GA4 property {GA4_PROPERTY_ID}...")
    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE_PATH,
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    client  = BetaAnalyticsDataClient(credentials=credentials)
    request = RunReportRequest(
        property    = f"properties/{GA4_PROPERTY_ID}",
        dimensions  = [Dimension(name="sessionGoogleAdsQuery")],
        metrics     = [
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="advertiserAdCost"),
        ],
        date_ranges = [DateRange(start_date=START_DATE, end_date=END_DATE)],
        limit       = MAX_ROWS,
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        rows.append({
            'session_google_ads_query': row.dimension_values[0].value,
            'sessions':                 int(row.metric_values[0].value),
            'engaged_sessions':         int(row.metric_values[1].value),
            'advertiser_ad_cost':       float(row.metric_values[2].value),
        })
    df = pd.DataFrame(rows)
    print(f"Pulled {len(df):,} queries from GA4")
    return df


# ============================================================
# Load and prepare data
# ============================================================

df = pull_ga4_data()
df = df[df['sessions'] > 0]
df = df.dropna(subset=['session_google_ads_query'])
df = df[df['session_google_ads_query'] != '(not set)']

for phrase, replacement in PROTECTED_PHRASES.items():
    df['session_google_ads_query'] = df['session_google_ads_query'].str.replace(
        phrase, replacement, case=False, regex=False
    )

df['bounces']     = df['sessions'] - df['engaged_sessions']
df['bounce_rate'] = df['bounces'] / df['sessions']

print(f"Loaded {len(df):,} queries after filtering")


# ============================================================
# Tokenize
# ============================================================

long_data_rows = []

for _, row in df.iterrows():
    query = str(row['session_google_ads_query'])
    words = [w.lower().strip('.,!?()[]"\'-') for w in query.split()]
    words = [
        w for w in words
        if w not in ALL_STOPWORDS
        and w not in BRAND_TERMS
        and not re.search(r'\d', w)
        and len(w) > 1
    ]
    for word in words:
        long_data_rows.append({
            'session_google_ads_query': str(row['session_google_ads_query']),
            'word':                     word,
            'advertiser_ad_cost':       row['advertiser_ad_cost'],
            'sessions':                 row['sessions'],
            'bounces':                  row['bounces'],
            'bounce_rate':              row['bounce_rate']
        })

long_data             = pd.DataFrame(long_data_rows)
long_data['sessions'] = pd.to_numeric(
    long_data['sessions'], errors='coerce'
).fillna(0).astype(int)
long_data = long_data[long_data['sessions'] > 0]

print(f"Tokenized into {len(long_data):,} word-level rows")


# ============================================================
# Aggregate to word level
# ============================================================

word_data = long_data.groupby('word').agg(
    advertiser_ad_cost       = ('advertiser_ad_cost', 'sum'),
    sessions                 = ('sessions', 'sum'),
    bounces                  = ('bounces', 'sum'),
    query_count              = ('session_google_ads_query', 'nunique')
).reset_index()

word_data['bounce_rate'] = word_data['bounces'] / word_data['sessions']
word_data = word_data[word_data['sessions'] > 0]
word_data = word_data.sort_values(
    by=['bounce_rate', 'sessions'], ascending=[False, False]
)

print(f"Aggregated to {len(word_data):,} unique words")


# ============================================================
# Save output
# One file containing all words with their metrics.
# Load this into Google Sheets and connect Looker Studio to it.
# Filter and explore directly in the dashboard.
# ============================================================

word_data.to_csv(OUTPUT_PATH, index=False)
print(f"\nSaved to: {OUTPUT_PATH}")
print("Load this file into Google Sheets to connect to your Looker Studio dashboard.")
