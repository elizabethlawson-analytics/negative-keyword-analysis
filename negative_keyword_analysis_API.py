# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# Python — GA4 Data API Version (No Manual Export Required)
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Identifies potential negative keywords by breaking paid search
#   queries into individual words and aggregating GA4 post-click
#   behavioral metrics (bounce rate, sessions, cost) at the word level.
#
#   This version pulls data directly from the GA4 Data API.
#   No manual export or Looker Studio access required.
#
# Requirements:
#   pip install pandas google-analytics-data
#
# Setup:
#   1. Create a Google Cloud project if you don't have one
#      https://console.cloud.google.com
#
#   2. Enable the Google Analytics Data API
#      https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
#
#   3. Create a service account and download the JSON key
#      https://console.cloud.google.com/iam-admin/serviceaccounts
#
#   4. Grant the service account Viewer access to your GA4 property
#      In GA4: Admin → Account Access Management → Add users
#
#   5. Update the configuration below with your:
#      - Path to your JSON key file
#      - GA4 property ID (found in GA4 Admin → Property Settings)
#
# Note:
#   The GA4 Data API does not support the Google Analytics demo account.
#   Use negative_keyword_analysis_manual.py with sample_data.csv
#   to test with demo data.
#
# For the manual export version (no API required) see:
#   negative_keyword_analysis_manual.py
# ============================================================

import pandas as pd
import re
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
)
from google.oauth2 import service_account

# ============================================================
# Configuration
# ============================================================

# Path to your service account JSON key file
KEY_FILE_PATH = 'your-service-account-key.json'

# Your GA4 property ID (numeric, found in GA4 Admin → Property Settings)
# Format: '123456789' (do not include the 'properties/' prefix)
GA4_PROPERTY_ID = 'your-ga4-property-id'

# Date range for analysis
# Format: 'YYYY-MM-DD' or use relative dates like 'today', '90daysAgo'
START_DATE = '90daysAgo'
END_DATE   = 'today'

# Maximum number of rows to pull from GA4
# GA4 API returns up to 250,000 rows per request
MAX_ROWS = 10000

# ============================================================
# Brand terms
# Replace with your own brand name(s) and all variations.
# ============================================================
BRAND_TERMS = {
    'your_brand_name',          # Replace with your actual brand name
    # 'your_brand_abbreviation',
    # 'common_misspelling',
}

# ============================================================
# Protected phrases
# Multi-word terms to treat as a single token.
# ============================================================
PROTECTED_PHRASES = {
    # 'myasthenia gravis': 'myasthenia_gravis',
    # 'rolls royce':       'rolls_royce',
}

# Thresholds
MIN_SESSIONS          = 3
BOUNCE_RATE_THRESHOLD = 0.50

# Output files
OUTPUT_WORDS_PATH   = 'high_bounce_words.csv'
OUTPUT_QUERIES_PATH = 'flagged_queries.csv'

# Column names (used throughout — do not change)
QUERY_COLUMN    = 'Session Google Ads query'
SESSIONS_COLUMN = 'Sessions'
ENGAGED_COLUMN  = 'Engaged sessions'
COST_COLUMN     = 'Ads cost'


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
    """
    Pull paid search query data directly from the GA4 Data API.
    Returns a DataFrame with query, sessions, engaged sessions, and cost.
    """
    print(f"Connecting to GA4 property {GA4_PROPERTY_ID}...")

    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE_PATH,
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[
            Dimension(name="sessionGoogleAdsQuery"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="advertiserAdCost"),
        ],
        date_ranges=[
            DateRange(start_date=START_DATE, end_date=END_DATE)
        ],
        # Only include rows where a Google Ads query exists
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="sessionGoogleAdsQuery",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value="(not set)",
                    case_sensitive=False,
                )
            ),
            not_expression=FilterExpression(
                filter=Filter(
                    field_name="sessionGoogleAdsQuery",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value="(not set)",
                        case_sensitive=False,
                    )
                )
            )
        ),
        limit=MAX_ROWS,
    )

    response = client.run_report(request)

    rows = []
    for row in response.rows:
        rows.append({
            QUERY_COLUMN:    row.dimension_values[0].value,
            SESSIONS_COLUMN: int(row.metric_values[0].value),
            ENGAGED_COLUMN:  int(row.metric_values[1].value),
            COST_COLUMN:     float(row.metric_values[2].value),
        })

    df = pd.DataFrame(rows)
    print(f"Pulled {len(df):,} queries from GA4")
    return df


# ============================================================
# Load and prepare data
# ============================================================

df = pull_ga4_data()
df = df[df[SESSIONS_COLUMN] > 0]
df = df.dropna(subset=[QUERY_COLUMN])
df = df[df[QUERY_COLUMN] != '(not set)']

for phrase, replacement in PROTECTED_PHRASES.items():
    df[QUERY_COLUMN] = df[QUERY_COLUMN].str.replace(
        phrase, replacement, case=False, regex=False
    )

df['Bounces']     = df[SESSIONS_COLUMN] - df[ENGAGED_COLUMN]
df['Bounce Rate'] = df['Bounces'] / df[SESSIONS_COLUMN]

print(f"Loaded {len(df):,} queries after filtering")


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
            QUERY_COLUMN:    str(row[QUERY_COLUMN]),
            'Word':          word,
            COST_COLUMN:     row.get(COST_COLUMN, 0),
            SESSIONS_COLUMN: row[SESSIONS_COLUMN],
            'Bounces':       row['Bounces'],
            'Bounce Rate':   row['Bounce Rate']
        })

long_data = pd.DataFrame(long_data_rows)
long_data[SESSIONS_COLUMN] = pd.to_numeric(
    long_data[SESSIONS_COLUMN], errors='coerce'
).fillna(0).astype(int)
long_data = long_data[long_data[SESSIONS_COLUMN] > 0]

print(f"Tokenized into {len(long_data):,} word-level rows")


# ============================================================
# Aggregate to word level
# ============================================================

word_data = long_data.groupby('Word').agg(
    Cost        = (COST_COLUMN, 'sum'),
    Sessions    = (SESSIONS_COLUMN, 'sum'),
    Bounces     = ('Bounces', 'sum'),
    Query_Count = (QUERY_COLUMN, 'nunique')
).reset_index()

word_data.rename(columns={'Cost': COST_COLUMN}, inplace=True)
word_data['Bounce Rate'] = word_data['Bounces'] / word_data['Sessions']
word_data = word_data[word_data['Sessions'] > 0]
word_data = word_data.sort_values(
    by=['Bounce Rate', 'Sessions'], ascending=[False, False]
)

print(f"Aggregated to {len(word_data):,} unique words")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce = word_data[
    (word_data['Bounce Rate'] >= BOUNCE_RATE_THRESHOLD) &
    (word_data['Sessions']    >= MIN_SESSIONS)
].copy()

print(f"\nFlagged {len(high_bounce):,} words with bounce rate >= "
      f"{BOUNCE_RATE_THRESHOLD:.0%} and >= {MIN_SESSIONS} sessions:")
print(high_bounce.to_string(index=False))

high_bounce.to_csv(OUTPUT_WORDS_PATH, index=False)
print(f"\nSaved to: {OUTPUT_WORDS_PATH}")
print(f"Load this file into Google Sheets to connect to your Looker Studio dashboard.")

flagged_words   = set(high_bounce['Word'].str.lower())
flagged_queries = long_data[
    long_data['Word'].str.lower().isin(flagged_words)
].sort_values(by=[SESSIONS_COLUMN, 'Bounce Rate'], ascending=[False, False])
flagged_queries.to_csv(OUTPUT_QUERIES_PATH, index=False)
print(f"Query detail saved to: {OUTPUT_QUERIES_PATH}")


# ============================================================
# Helper: investigate a specific word
# ============================================================

def investigate_word(word):
    """
    Print all queries containing a specific word.
    Usage: investigate_word('garage')
    """
    subset = long_data[long_data['Word'].str.lower() == word.lower()]
    if subset.empty:
        print(f"No queries found containing '{word}'")
        return
    print(f"\n=== '{word}' ===")
    print(f"Sessions:    {subset[SESSIONS_COLUMN].sum():,}")
    print(f"Bounce rate: {subset['Bounces'].sum() / subset[SESSIONS_COLUMN].sum():.1%}")
    print(f"Cost:        ${subset[COST_COLUMN].sum():,.2f}")
    print(subset[[QUERY_COLUMN, SESSIONS_COLUMN, 'Bounce Rate', COST_COLUMN]]
          .sort_values(SESSIONS_COLUMN, ascending=False).to_string(index=False))

# investigate_word('garage')
# investigate_word('pixel')
