-- ============================================================
-- Negative Keyword Analysis: Word-Level Post-Click Behavior
-- ============================================================
-- Author: Elizabeth Lawson
-- Description:
--   Identifies potential negative keywords by breaking paid search
--   queries into individual words and aggregating GA4 post-click
--   behavioral metrics (bounce rate, sessions, cost) at the word level.
--
--   Words with high bounce rates and meaningful spend surface
--   unqualified traffic patterns that traditional query-level
--   review consistently misses.
--
-- Requirements:
--   - Google Ads data connected to BigQuery via Data Transfer Service
--   - GA4 data exported to BigQuery via native GA4 BigQuery export
--   - Both datasets in the same BigQuery project
--
-- Usage:
--   Replace the following placeholders with your actual values:
--   - your-project-id       → your GCP project ID
--   - your_ga4_dataset      → your GA4 BigQuery export dataset name
--   - your_ads_dataset      → your Google Ads BigQuery dataset name
--   - your_properties_table → optional: table mapping property names to display names
-- ============================================================


CREATE OR REPLACE TABLE `your-project-id.your_ga4_dataset.negative_keyword_research` AS

WITH src AS (
  SELECT
    date,
    property,
    session_google_ads_query,
    advertiser_ad_cost,
    sessions,
    engaged_sessions
  FROM `your-project-id.your_ga4_dataset.custom_sessions_google_ads_keywords`
  -- Note: This table is created by joining GA4 BigQuery export data with
  -- Google Ads cost data via shared GCLID. See README for setup instructions.
),

prepped AS (
  SELECT
    date,
    property,
    session_google_ads_query,
    advertiser_ad_cost,
    sessions,
    engaged_sessions,
    (sessions - engaged_sessions)                                AS bounces,
    SAFE_DIVIDE(sessions - engaged_sessions, NULLIF(sessions,0)) AS bounce_rate,

    -- Protect multi-word domain-specific terms from being split into individual words.
    -- Add additional replacements here for terms specific to your industry.
    -- Example: REPLACE('myasthenia gravis', 'myasthenia_gravis')
    -- Replace spaces with underscores so the phrase is treated as a single token.
    session_google_ads_query AS query_fixed

    -- Uncomment and adapt the line below for your own multi-word terms:
    -- REPLACE(session_google_ads_query, 'your multi word term', 'your_multi_word_term') AS query_fixed

  FROM src
  WHERE sessions IS NOT NULL AND sessions > 0
),

-- ============================================================
-- Stopwords list
-- Add or remove words based on your industry and use case.
-- This list covers common English stopwords that would appear
-- in search queries but carry no intent signal on their own.
-- ============================================================
stopwords AS (
  SELECT [
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
    'can','will','just','don','should','now'
  ] AS sw
),

-- ============================================================
-- Tokenization
-- Split each query into individual words and join with metrics.
-- Stopwords are filtered out at this stage.
-- ============================================================
tokens AS (
  SELECT
    p.date,
    p.property,
    p.session_google_ads_query,
    p.advertiser_ad_cost,
    p.sessions,
    p.bounces,
    p.bounce_rate,
    word
  FROM prepped p
  CROSS JOIN UNNEST(SPLIT(p.query_fixed, ' ')) AS word
  CROSS JOIN stopwords
  WHERE LOWER(word) NOT IN (SELECT * FROM UNNEST(sw))
    AND word IS NOT NULL
    AND word != ''
)

-- ============================================================
-- Final output
-- One row per word per session/query combination.
-- Join with a properties lookup table if available to add
-- human-readable property/account names.
-- ============================================================
SELECT
  date,
  t.property,
  display_name,           -- from properties lookup; remove if not applicable
  session_google_ads_query,
  word,
  advertiser_ad_cost,
  sessions,
  bounces,
  bounce_rate
FROM tokens t

-- Optional: join a properties table to get display names
-- Remove this join if you don't have a properties lookup table
LEFT JOIN `your-project-id.your_ads_dataset.properties` n
  ON t.property = n.name
