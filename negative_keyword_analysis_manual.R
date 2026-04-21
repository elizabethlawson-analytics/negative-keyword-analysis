# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# R — Manual Export Version
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
#   install.packages("readr")
#
# Output:
#   word_level_analysis.csv — all words with sessions, bounces, bounce_rate, cost

#
#   Both files use standardized column names matching the BigQuery
#   SQL version so all implementations connect to the same
#   Looker Studio dashboard without any field remapping.
#
# For the GA4 API version (no manual export required) see:
#   negative_keyword_analysis_api.R
# ============================================================

library(readr)

# ============================================================
# Configuration
# ============================================================

DATA_PATH <- "sample_data.csv"

# Column names in your input file
# Looker Studio export: COST_COLUMN <- "Ads cost"
# GA4 Explore export:   COST_COLUMN <- "Google Ads cost"
QUERY_COLUMN    <- "Session Google Ads query"
SESSIONS_COLUMN <- "Sessions"
ENGAGED_COLUMN  <- "Engaged sessions"
COST_COLUMN     <- "Ads cost"

# Number of header rows to skip
# Looker Studio exports: 0
# Raw GA4 Explore exports: 6
SKIP_ROWS <- 0

# ============================================================
# Brand terms
# ============================================================
BRAND_TERMS <- c(
  "your_brand_name"
  # "your_brand_abbreviation"
)

# ============================================================
# Protected phrases
# ============================================================
PROTECTED_PHRASES <- c(
  # "myasthenia gravis" = "myasthenia_gravis",
  # "rolls royce"       = "rolls_royce"
)

MIN_SESSIONS <- 3
OUTPUT_PATH  <- "word_level_analysis.csv"


# ============================================================
# Stopwords
# ============================================================

ENGLISH_STOPWORDS <- c(
  "i","me","my","myself","we","our","ours","ourselves",
  "you","your","yours","yourself","yourselves",
  "he","him","his","himself","she","her","hers","herself",
  "it","its","itself","they","them","their","theirs","themselves",
  "what","which","who","whom","this","that","these","those",
  "am","is","are","was","were","be","been","being",
  "have","has","had","having","do","does","did","doing",
  "a","an","the","and","but","if","or","because","as","until",
  "while","of","at","by","for","with","about","against",
  "between","into","through","during","before","after",
  "above","below","to","from","up","down","in","out",
  "on","off","over","under","again","further","then","once",
  "here","there","when","where","why","how",
  "all","any","both","each","few","more","most","other",
  "some","such","no","nor","not","only","own","same",
  "so","than","too","very","s","t",
  "can","will","just","don","should","now",
  "store","stores","near","me","shop","buy","online","get",
  "best","cheap","affordable","new","used"
)

SPANISH_STOPWORDS <- c(
  "de","la","el","en","y","a","los","las","un","una","es",
  "por","con","no","su","para","como","pero","sus","le",
  "ya","o","porque","cuando","muy","sin","sobre","también",
  "hasta","hay","donde","quien","desde","todo","nos",
  "durante","eso","mi","del","se","lo","da","si","al","e",
  "cerca","tienda"
)

US_STATES <- c(
  "alabama","alaska","arizona","arkansas","california","colorado",
  "connecticut","delaware","florida","georgia","hawaii","idaho",
  "illinois","indiana","iowa","kansas","kentucky","louisiana",
  "maine","maryland","massachusetts","michigan","minnesota",
  "mississippi","missouri","montana","nebraska","nevada",
  "hampshire","jersey","mexico","york","carolina","dakota",
  "ohio","oklahoma","oregon","pennsylvania","rhode","island",
  "tennessee","texas","utah","vermont","virginia","washington",
  "wisconsin","wyoming",
  "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id",
  "il","in","ia","ks","ky","la","me","md","ma","mi","mn","ms",
  "mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
  "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv",
  "wi","wy","dc"
)

ALL_STOPWORDS <- unique(c(ENGLISH_STOPWORDS, SPANISH_STOPWORDS, US_STATES))


# ============================================================
# Load and prepare data
# ============================================================

df <- read_csv(DATA_PATH, skip = SKIP_ROWS, show_col_types = FALSE)
df <- df[!is.na(df[[QUERY_COLUMN]]), ]

if (length(PROTECTED_PHRASES) > 0) {
  for (i in seq_along(PROTECTED_PHRASES)) {
    df[[QUERY_COLUMN]] <- gsub(
      names(PROTECTED_PHRASES)[i],
      PROTECTED_PHRASES[i],
      df[[QUERY_COLUMN]], ignore.case = TRUE
    )
  }
}

df$bounces     <- df[[SESSIONS_COLUMN]] - df[[ENGAGED_COLUMN]]
df$bounce_rate <- df$bounces / df[[SESSIONS_COLUMN]]

cat("Loaded", nrow(df), "queries\n")


# ============================================================
# Tokenize
# ============================================================

long_data <- NULL
itime     <- proc.time()[3]

for (i in 1:nrow(df)) {
  query <- tolower(as.character(df[[QUERY_COLUMN]][i]))
  words <- unlist(strsplit(query, " "))
  words <- gsub("[.,!?()\\[\\]\"'\\-]", "", words)
  words <- words[
    !words %in% ALL_STOPWORDS &
    !words %in% BRAND_TERMS &
    !grepl("[[:digit:]]", words) &
    nchar(words) > 1 &
    words != ""
  ]
  if (length(words) > 0) {
    temp <- data.frame(
      session_google_ads_query = as.character(df[[QUERY_COLUMN]][i]),
      word                     = words,
      advertiser_ad_cost       = df[[COST_COLUMN]][i],
      sessions                 = df[[SESSIONS_COLUMN]][i],
      bounces                  = df$bounces[i],
      bounce_rate              = df$bounce_rate[i],
      stringsAsFactors         = FALSE
    )
    long_data <- rbind(long_data, temp)
  }
  if (i %% 100 == 0) {
    ctime     <- proc.time()[3]
    timetoend <- ((ctime - itime) / i) * (nrow(df) - i)
    cat(i, "of", nrow(df),
        "| Est. remaining:", round(timetoend / 60, 1), "min\n")
  }
}

long_data <- long_data[long_data$sessions > 0, ]
cat("Tokenized into", nrow(long_data), "word-level rows\n")


# ============================================================
# Aggregate to word level
# ============================================================

# ============================================================
# Save output
# One row per word per query — Looker Studio handles aggregation.
# Load this into Google Sheets and connect Looker Studio to it.
# ============================================================

output <- long_data[, c(
  "word",
  "session_google_ads_query",
  "sessions",
  "bounces",
  "bounce_rate",
  "advertiser_ad_cost"
)]

output <- output[order(output$word, -output$sessions), ]

write_csv(output, OUTPUT_PATH)
cat("Saved", nrow(output), "rows to:", OUTPUT_PATH, "
")
cat("Load this file into Google Sheets to connect to your Looker Studio dashboard.
")
