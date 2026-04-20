# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# Python Implementation (Manual / No BigQuery Required)
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Identifies potential negative keywords by breaking paid search
#   queries into individual words and aggregating GA4 post-click
#   behavioral metrics (bounce rate, sessions, cost) at the word level.
#
#   This version uses a CSV exported directly from a Looker Studio
#   report connected to GA4. No BigQuery or data engineering required.
#
# Requirements:
#   pip install pandas
#
# Data Setup:
#   Option A: Export from Looker Studio
#     1. Open your Looker Studio report connected to GA4
#     2. Add a table with these fields:
#          Dimension: Session Google Ads query
#          Metrics:   Sessions, Engaged sessions, Ads cost
#     3. Click the three dots on the table → Export → CSV
#     4. Update DATA_PATH below to point to that file
#
#   Option B: Export from GA4 Explore
#     1. Create a Free Form exploration in GA4 with:
#          Dimension: Session Google Ads query
#          Metrics:   Sessions, Engaged sessions, Google Ads cost
#     2. Export as CSV (requires Editor access on the GA4 property)
#     3. Set SKIP_ROWS = 6 for raw GA4 exports
#     4. Set COST_COLUMN = 'Google Ads cost'
#
#   A sample dataset (sample_data.csv) is included in this repo
#   so you can run the analysis immediately without your own data.
#
# Usage:
#   1. Update DATA_PATH to point to your exported CSV
#   2. Update BRAND_TERMS with your brand name(s)
#   3. Adjust BOUNCE_RATE_THRESHOLD and MIN_SESSIONS as needed
#   4. Run the script
#   5. Review output in high_bounce_words.csv and flagged_queries.csv
#   6. Load high_bounce_words.csv into Google Sheets
#   7. Connect your Looker Studio dashboard to that Google Sheet
# ============================================================

import pandas as pd
import re

# ============================================================
# Configuration — update these values for your use case
# ============================================================

# Path to your Looker Studio or GA4 export CSV
DATA_PATH = 'sample_data.csv'

# Column names — update if your export uses different names
QUERY_COLUMN    = 'Session Google Ads query'  # Looker Studio export format
SESSIONS_COLUMN = 'Sessions'
ENGAGED_COLUMN  = 'Engaged sessions'
COST_COLUMN     = 'Ads cost'                  # Use 'Google Ads cost' for GA4 direct export

# Number of header rows to skip
# Looker Studio exports: 0
# Raw GA4 exports: typically 6
SKIP_ROWS = 0

# ============================================================
# Brand terms
# Replace these with your own brand name(s).
# Words in this list will be removed from queries before analysis
# so that branded terms don't distort the word-level results.
# Add all variations: full name, abbreviations, common misspellings.
# ============================================================
BRAND_TERMS = {
    'google',       # Replace with your brand name
    # Add more variations:
    # 'your brand abbreviation',
    # 'common misspelling',
}

# ============================================================
# Multi-word phrases to protect from splitting
# Add phrases that should be treated as a single token.
# Format: {'original phrase': 'replacement_with_underscores'}
# ============================================================
PROTECTED_PHRASES = {
    # Healthcare examples:
    # 'myasthenia gravis': 'myasthenia_gravis',
    # 'lambert eaton':     'lambert_eaton',
    # Automotive examples:
    # 'rolls royce':       'rolls_royce',
    # 'alfa romeo':        'alfa_romeo',
    # Add your own:
}

# Minimum sessions for a word to be included in output
MIN_SESSIONS = 3

# Bounce rate threshold for flagging words as potential negatives
BOUNCE_RATE_THRESHOLD = 0.50

# Output file paths
OUTPUT_WORDS_PATH   = 'high_bounce_words.csv'
OUTPUT_QUERIES_PATH = 'flagged_queries.csv'


# ============================================================
# Stopwords
# Covers English, Spanish, US geography, and common search terms.
# Add or remove based on your industry and use case.
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
    'por','con','no','una','su','para','como','más','pero',
    'sus','le','ya','o','porque','cuando','muy','sin','sobre',
    'también','me','hasta','hay','donde','quien','desde','todo',
    'nos','durante','estados','todo','eso','las','mi','del',
    'se','lo','le','da','si','al','e'
}

# US state names and abbreviations
# Removes geographic modifiers common in local search campaigns
US_STATES = {
    'alabama','alaska','arizona','arkansas','california','colorado',
    'connecticut','delaware','florida','georgia','hawaii','idaho',
    'illinois','indiana','iowa','kansas','kentucky','louisiana',
    'maine','maryland','massachusetts','michigan','minnesota',
    'mississippi','missouri','montana','nebraska','nevada',
    'hampshire','jersey','mexico','york','carolina','dakota',
    'ohio','oklahoma','oregon','pennsylvania','rhode','island',
    'tennessee','texas','utah','vermont','virginia','washington',
    'virginia','wisconsin','wyoming',
    # Abbreviations
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

# Remove unnamed columns
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Remove rows with no query data
df = df.dropna(subset=[QUERY_COLUMN])

# Apply protected phrase replacements before tokenizing
for phrase, replacement in PROTECTED_PHRASES.items():
    df[QUERY_COLUMN] = df[QUERY_COLUMN].str.replace(
        phrase, replacement, case=False, regex=False
    )

# Calculate bounces and bounce rate
df['Bounces']     = df[SESSIONS_COLUMN] - df[ENGAGED_COLUMN]
df['Bounce Rate'] = df['Bounces'] / df[SESSIONS_COLUMN]

print(f"Loaded {len(df):,} queries")
print(df.head())


# ============================================================
# Tokenize: break each query into individual words
# ============================================================

long_data_rows = []

for _, row in df.iterrows():
    query = str(row[QUERY_COLUMN])

    # Split into words
    words = query.lower().split()

    # Clean punctuation from each word
    words = [w.strip('.,!?()[]"\'-') for w in words]

    # Filter: remove stopwords, brand terms, numbers, empty strings
    words = [
        w for w in words
        if w not in ALL_STOPWORDS          # not a stopword
        and w not in BRAND_TERMS           # not a brand term
        and not re.search(r'\d', w)        # contains no digits
        and len(w) > 1                     # more than 1 character
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

# Ensure Sessions is numeric
long_data[SESSIONS_COLUMN] = pd.to_numeric(
    long_data[SESSIONS_COLUMN], errors='coerce'
).fillna(0).astype(int)

# Filter out zero-session rows
long_data = long_data[long_data[SESSIONS_COLUMN] > 0]

print(f"\nTokenized into {len(long_data):,} word-level rows")


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

print(f"\nAggregated to {len(word_data):,} unique words")


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
print(f"\nWord-level output saved to: {OUTPUT_WORDS_PATH}")
print(f"Load this file into Google Sheets to connect to your Looker Studio dashboard.")


# ============================================================
# Drill into flagged words — show associated queries
# ============================================================

flagged_words = set(high_bounce['Word'].str.lower())

flagged_queries = long_data[
    long_data['Word'].str.lower().isin(flagged_words)
].copy()

flagged_queries = flagged_queries.sort_values(
    by=[SESSIONS_COLUMN, 'Bounce Rate'], ascending=[False, False]
)

flagged_queries.to_csv(OUTPUT_QUERIES_PATH, index=False)
print(f"Query-level detail saved to: {OUTPUT_QUERIES_PATH}")


# ============================================================
# Helper: investigate a specific word
# ============================================================

def investigate_word(word):
    """
    Print all queries containing a specific flagged word,
    along with their sessions, bounce rate, and cost.

    Usage: investigate_word('pixel')
           investigate_word('garage')
    """
    subset = long_data[long_data['Word'].str.lower() == word.lower()]
    if subset.empty:
        print(f"No queries found containing '{word}'")
        return

    print(f"\n=== Queries containing '{word}' ===")
    print(f"Total sessions:      {subset[SESSIONS_COLUMN].sum():,}")
    print(f"Overall bounce rate: "
          f"{subset['Bounces'].sum() / subset[SESSIONS_COLUMN].sum():.1%}")
    print(f"Total cost:          ${subset[COST_COLUMN].sum():,.2f}")
    print(f"\nIndividual queries:")
    print(subset[[QUERY_COLUMN, SESSIONS_COLUMN,
                  'Bounce Rate', COST_COLUMN]]
          .sort_values(SESSIONS_COLUMN, ascending=False)
          .to_string(index=False))


# Example usage — uncomment to investigate specific words:
# investigate_word('pixel')
# investigate_word('garage')
# investigate_word('usa')
