-- ============================================================
-- Negative Keyword Analysis: Word-Level Post-Click Behavior
-- SQL / BigQuery Implementation (Fully Automated)
-- ============================================================
-- Author: Elizabeth Lawson
-- Description:
--   Identifies potential negative keywords by breaking paid search
--   queries into individual words and aggregating GA4 post-click
--   behavioral metrics (bounce rate, sessions, cost) at the word level.
--
--   This is the fully automated version. Once the two pipelines are
--   set up, this view stays current automatically with no manual
--   maintenance required.
--
-- Requirements:
--   Pipeline 1: Google Ads → BigQuery via Google Ads Data Transfer Service
--     https://cloud.google.com/bigquery-transfer/docs/google-ads-transfer
--
--   Pipeline 2: GA4 → BigQuery via native GA4 BigQuery export
--     https://support.google.com/analytics/answer/9823238
--
-- Setup:
--   Replace the following placeholders with your actual values:
--   - your-project-id        → your GCP project ID
--   - your_ga4_dataset       → your GA4 BigQuery export dataset name
--   - your_ads_dataset       → your Google Ads BigQuery dataset name
--   - your_properties_table  → optional table mapping property names to display names
--
-- Usage:
--   1. Replace all placeholders below with your actual project/dataset names
--   2. Update BRAND_TERMS with your brand name(s)
--   3. Update PROTECTED_PHRASES with any multi-word terms to preserve
--   4. Run in BigQuery to create the view
--   5. Connect the resulting table directly to Looker Studio
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
  -- Google Ads cost data. See README for setup instructions.
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

    -- ============================================================
    -- PROTECTED PHRASES
    -- Replace spaces with underscores in multi-word terms so they
    -- are treated as a single token during tokenization.
    -- Add your own industry-specific phrases here.
    -- ============================================================
    REPLACE(
    REPLACE(
    REPLACE(
      session_google_ads_query,
      -- Healthcare examples:
      'myasthenia gravis', 'myasthenia_gravis'),
      -- Automotive examples (uncomment as needed):
      -- 'rolls royce', 'rolls_royce'),
      -- 'alfa romeo', 'alfa_romeo'),
      -- Add your own:
      'lambert eaton', 'lambert_eaton')
    AS query_fixed

  FROM src
  WHERE sessions IS NOT NULL AND sessions > 0
),

-- ============================================================
-- STOPWORDS
-- Covers English, Spanish, US geography, and common search terms.
-- Add or remove words based on your industry and use case.
-- ============================================================
stopwords AS (
  SELECT [
    -- English stopwords
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
    -- Common search modifier terms
    'store','stores','near','me','shop','buy','online','get',
    'best','cheap','affordable','new','used',

    -- Spanish stopwords
    -- Remove if your campaigns do not target Spanish-speaking audiences
    'de','la','el','en','y','a','los','las','un','una','es',
    'por','con','no','su','para','como','pero','sus','le',
    'ya','o','porque','cuando','muy','sin','sobre','también',
    'hasta','hay','donde','quien','desde','todo','nos',
    'durante','eso','mi','del','se','lo','da','si','al','e',
    'cerca','tienda',

    -- US state names
    -- Remove if your campaigns are not geographically targeted
    'alabama','alaska','arizona','arkansas','california','colorado',
    'connecticut','delaware','florida','georgia','hawaii','idaho',
    'illinois','indiana','iowa','kansas','kentucky','louisiana',
    'maine','maryland','massachusetts','michigan','minnesota',
    'mississippi','missouri','montana','nebraska','nevada',
    'hampshire','jersey','mexico','york','carolina','dakota',
    'ohio','oklahoma','oregon','pennsylvania','rhode','island',
    'tennessee','texas','utah','vermont','virginia','washington',
    'wisconsin','wyoming',

    -- US state abbreviations
    'al','ak','az','ar','ca','co','ct','fl','ga','hi','id',
    'il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms',
    'mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok',
    'or','pa','ri','sc','sd','tn','tx','ut','vt','wa','wv',
    'wi','wy','dc',

    -- ============================================================
    -- BRAND TERMS
    -- Add your brand name(s) and all common variations here.
    -- These will be removed from queries before analysis so branded
    -- terms do not distort the word-level results.
    -- ============================================================
    'your_brand_name'        -- Replace with your actual brand name
    -- 'your_brand_abbreviation',
    -- 'common_misspelling'

  ] AS sw
),

-- ============================================================
-- TOKENIZATION
-- Split each query into individual words and join with metrics.
-- Stopwords, brand terms, and numeric tokens are filtered out.
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
    AND NOT REGEXP_CONTAINS(word, r'\d')  -- remove words containing digits
    AND LENGTH(word) > 1                  -- remove single characters
)

-- ============================================================
-- FINAL OUTPUT
-- One row per word per query combination.
-- Looker Studio handles all aggregation.
-- Connect this table directly to Looker Studio.
-- ============================================================
SELECT
  date,
  property,
  word,
  session_google_ads_query,
  sessions,
  bounces,
  bounce_rate,
  advertiser_ad_cost
FROM tokens
ORDER BY word, sessions DESC
