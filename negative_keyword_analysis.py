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
#   This version uses a manually exported GA4 CSV report.
#   No BigQuery or data engineering setup required.
#
# Requirements:
#   pip install pandas nltk
#
# Data Setup:
#   Export a custom GA4 Explore report with the following fields:
#     Dimensions: Session Google Ads query
#     Metrics:    Sessions, Engaged sessions, Google Ads cost
#   Save the export as a CSV file and update DATA_PATH below.
#
#   A sample dataset (sample_data.csv) is included in this repo
#   so you can run the analysis immediately without your own data.
#
# Usage:
#   1. Update DATA_PATH to point to your exported CSV
#   2. Adjust BOUNCE_RATE_THRESHOLD and MIN_SESSIONS as needed
#   3. Run the script
#   4. Review output in high_bounce_words.csv and flagged_queries.csv
# ============================================================

import pandas as pd
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')

# ============================================================
# Configuration — update these values for your use case
# ============================================================

# Path to your GA4 export CSV
# Use 'sample_data.csv' to run with the included sample dataset
DATA_PATH = 'sample_data.csv'

# Number of header rows to skip in the GA4 export
# GA4 exports typically include 6 rows of metadata before the data
SKIP_ROWS = 0  # Set to 6 if using a raw GA4 export

# Words that should be kept together as a single token
# Add multi-word terms specific to your industry
# Format: {'original phrase': 'replacement_with_underscores'}
PROTECTED_PHRASES = {
    # 'myasthenia gravis': 'myasthenia_gravis',
    # 'add your': 'add_your',
    # 'multi word terms': 'multi_word_terms',
}

# Minimum sessions for a word to be included in output
# Increase this to filter out low-volume noise
MIN_SESSIONS = 3

# Bounce rate threshold for flagging words as potential negatives
# Words above this threshold with sufficient sessions will be flagged
BOUNCE_RATE_THRESHOLD = 0.50

# Output file paths
OUTPUT_WORDS_PATH = 'high_bounce_words.csv'
OUTPUT_QUERIES_PATH = 'flagged_queries.csv'


# ============================================================
# Load and prepare data
# ============================================================

df = pd.read_csv(DATA_PATH, skiprows=SKIP_ROWS)

# Remove unnamed columns (common in GA4 exports)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Remove rows with no query data
df = df.dropna(subset=['Session Google Ads query'])

# Apply protected phrase replacements before tokenizing
for phrase, replacement in PROTECTED_PHRASES.items():
    df['Session Google Ads query'] = df['Session Google Ads query'].str.replace(
        phrase, replacement, case=False
    )

# Calculate bounces and bounce rate
# GA4 uses "Engaged sessions" — bounces = sessions that were not engaged
df['Bounces'] = df['Sessions'] - df['Engaged sessions']
df['Bounce Rate'] = df['Bounces'] / df['Sessions']

print(f"Loaded {len(df):,} queries")
print(df.head())


# ============================================================
# Tokenize: break each query into individual words
# ============================================================

stop_words = set(stopwords.words('english'))

long_data_rows = []

for _, row in df.iterrows():
    query = str(row['Session Google Ads query'])

    # Split query into words, removing stopwords
    words = [
        word for word in query.split()
        if word.lower() not in stop_words
        and word.strip() != ''
    ]

    for word in words:
        long_data_rows.append({
            'Session Google Ads query': query,
            'Word': word.lower(),
            'Google Ads cost': row.get('Google Ads cost', 0),
            'Sessions': row['Sessions'],
            'Bounces': row['Bounces'],
            'Bounce Rate': row['Bounce Rate']
        })

long_data = pd.DataFrame(long_data_rows)

# Ensure Sessions is numeric
long_data['Sessions'] = pd.to_numeric(
    long_data['Sessions'], errors='coerce'
).fillna(0).astype(int)

# Filter out zero-session rows
long_data = long_data[long_data['Sessions'] > 0]

print(f"\nTokenized into {len(long_data):,} word-level rows")


# ============================================================
# Aggregate to word level
# ============================================================

word_data = long_data.groupby('Word').agg(
    Google_Ads_Cost=('Google Ads cost', 'sum'),
    Sessions=('Sessions', 'sum'),
    Bounces=('Bounces', 'sum')
).reset_index()

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
    (word_data['Sessions'] >= MIN_SESSIONS)
].copy()

print(f"\nFlagged {len(high_bounce):,} words with bounce rate >= "
      f"{BOUNCE_RATE_THRESHOLD:.0%} and >= {MIN_SESSIONS} sessions")
print(high_bounce.head(10).to_string(index=False))

high_bounce.to_csv(OUTPUT_WORDS_PATH, index=False)
print(f"\nWord-level output saved to: {OUTPUT_WORDS_PATH}")


# ============================================================
# Drill into flagged words — show associated queries
# ============================================================

flagged_words = set(high_bounce['Word'].str.lower())

flagged_queries = long_data[
    long_data['Word'].str.lower().isin(flagged_words)
].copy()

flagged_queries = flagged_queries.sort_values(
    by=['Sessions', 'Bounce Rate'], ascending=[False, False]
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
    
    Usage: investigate_word('your_word_here')
    """
    subset = long_data[long_data['Word'].str.lower() == word.lower()]
    if subset.empty:
        print(f"No queries found containing '{word}'")
        return

    print(f"\n=== Queries containing '{word}' ===")
    print(f"Total sessions: {subset['Sessions'].sum():,}")
    print(f"Overall bounce rate: "
          f"{subset['Bounces'].sum() / subset['Sessions'].sum():.1%}")
    print(f"Total cost: ${subset['Google Ads cost'].sum():,.2f}")
    print(f"\nIndividual queries:")
    print(subset[['Session Google Ads query', 'Sessions',
                  'Bounce Rate', 'Google Ads cost']]
          .sort_values('Sessions', ascending=False)
          .to_string(index=False))

# Example usage:
# investigate_word('lamborghini')
# investigate_word('craigslist')
