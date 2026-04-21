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
    # Replace with your own brand name(s) and all variations.
    # These are removed before analysis so your branded queries
    # do not appear as flagged words in the output.
    #
    # Sample data uses brand1-brand15 as placeholders for real brand names.
    # When using your own data, replace these with your actual brand names.
    'brand1',  'brand2',  'brand3',  'brand4',  'brand5',
    'brand6',  'brand7',  'brand8',  'brand9',  'brand10',
    'brand11', 'brand12', 'brand13', 'brand14', 'brand15',
    # Add your own:
    # 'your_brand_name',
    # 'your_brand_abbreviation',
}

# ============================================================
# Protected phrases
# Multi-word terms that should be treated as a single token.
# Format: {'original phrase': 'replacement_with_underscores'}
# ============================================================
PROTECTED_PHRASES = {
    # Multi-word furniture terms that should be treated as a single token.
    # Without these, "lazy boy" would be split into "lazy" and "boy" separately.
    # Add any industry-specific multi-word terms relevant to your campaigns.

    # Brand/style names
    'lazy boy':         'lazy_boy',
    'la z boy':         'lazy_boy',
    'la-z-boy':         'lazy_boy',

    # Sofa/couch types
    'sofa bed':         'sofa_bed',
    'sleeper sofa':     'sleeper_sofa',
    'pull out sofa':    'pull_out_sofa',
    'pull out couch':   'pull_out_couch',
    'sofa sectional':   'sofa_sectional',
    'l shaped sofa':    'l_shaped_sofa',
    'l shaped couch':   'l_shaped_couch',
    'u shaped sofa':    'u_shaped_sofa',

    # Table types
    'coffee table':     'coffee_table',
    'end table':        'end_table',
    'side table':       'side_table',
    'dining table':     'dining_table',
    'kitchen table':    'kitchen_table',
    'console table':    'console_table',
    'accent table':     'accent_table',
    'sofa table':       'sofa_table',
    'night stand':      'night_stand',
    'night table':      'night_table',

    # Chair types
    'rocking chair':    'rocking_chair',
    'accent chair':     'accent_chair',
    'arm chair':        'arm_chair',
    'office chair':     'office_chair',
    'dining chair':     'dining_chair',
    'lounge chair':     'lounge_chair',
    'lift chair':       'lift_chair',
    'power recliner':   'power_recliner',
    'zero gravity':     'zero_gravity',

    # Bedroom
    'bed frame':        'bed_frame',
    'king size':        'king_size',
    'queen size':       'queen_size',
    'twin size':        'twin_size',
    'full size':        'full_size',
    'bunk bed':         'bunk_bed',
    'day bed':          'day_bed',
    'murphy bed':       'murphy_bed',
    'platform bed':     'platform_bed',
    'storage bed':      'storage_bed',

    # Dining
    'dining room':      'dining_room',
    'dining set':       'dining_set',
    'bar stool':        'bar_stool',
    'counter stool':    'counter_stool',

    # Living room
    'living room':      'living_room',
    'sectional sofa':   'sectional_sofa',
    'love seat':        'love_seat',

    # Storage
    'book case':        'bookcase',
    'book shelf':       'bookshelf',
    'tv stand':         'tv_stand',
    'media console':    'media_console',
    'chest of drawers': 'chest_of_drawers',
    'chest of drawer':  'chest_of_drawers',

    # Mattress
    'box spring':       'box_spring',
    'memory foam':      'memory_foam',
    'king mattress':    'king_mattress',
    'queen mattress':   'queen_mattress',

    # Other
    'grand piano':      'grand_piano',
    'home office':      'home_office',
    'patio furniture':  'patio_furniture',
    'outdoor furniture':'outdoor_furniture',
    'big lots':         'big_lots',
    'home depot':       'home_depot',
    'rooms to go':      'rooms_to_go',
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

# Rename columns by position to handle BOM characters or encoding
# issues that can corrupt column names when downloading from GitHub,
# Google Sheets, or Looker Studio exports.
df.columns = [QUERY_COLUMN, SESSIONS_COLUMN, ENGAGED_COLUMN, COST_COLUMN]

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
