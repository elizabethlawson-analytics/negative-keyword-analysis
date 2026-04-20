# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# R Implementation
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Identifies potential negative keywords by breaking paid search
#   queries into individual words and aggregating GA post-click
#   behavioral metrics (bounce rate, sessions, cost) at the word level.
#
#   This version pulls data directly from Google Analytics via the
#   Google Analytics Reporting API (Universal Analytics / GA3).
#   Because Google Ads is connected to GA, no session-level join
#   is required — cost and click data flow directly into GA sessions.
#
# Requirements:
#   install.packages(c("googleAnalyticsR", "stopwords", "qdap"))
#
# Setup:
#   1. Update DATE_RANGE to your desired analysis window
#   2. Update VIEW_NAME_PATTERN to match your GA view name
#   3. Update BRAND_TERMS, EXCLUDED_TERMS with your own lists
#      (or load them from Google Sheets — see notes below)
#   4. Update OUTPUT_PATH to your desired output location
#
# Notes:
#   - This script was originally written for Universal Analytics (GA3).
#     For GA4, use the Python version or SQL version in this repo.
#   - The R version includes more sophisticated preprocessing:
#     geographic term removal, brand detection, and Spanish stopwords.
#     These enhancements can be adapted to the Python/SQL versions.
# ============================================================


# ============================================================
# Configuration
# ============================================================

# Date range for analysis
DATE_RANGE <- c("2024-01-01", "2024-01-31")  # Update to your desired range

# Pattern to match your GA view name — update to match your account
VIEW_NAME_PATTERN <- "your view name"

# Output file path — update to your desired output location
OUTPUT_PATH <- "~/negative_keyword_output.csv"

# Bounce rate threshold for flagging words
BOUNCE_RATE_THRESHOLD <- 0.50

# Minimum sessions for a word to be included
MIN_SESSIONS <- 3

# ============================================================
# Brand/term lists
# These can be loaded from Google Sheets (see commented code below)
# or defined directly as vectors here.
# ============================================================

# Terms to exclude entirely from analysis
# Add industry-specific terms that are part of your targeting
# but should not be flagged as negative keywords
EXCLUDED_TERMS <- c(
  # "your_excluded_term_1",
  # "your_excluded_term_2"
)

# Brand terms — words that identify which brand/account the query belongs to
# Used to associate queries with the correct brand when it's not explicit
BRAND_TERMS <- data.frame(
  term  = c("your_brand_term"),   # the word as it appears in queries
  brand = c("your_brand_name")    # the brand it maps to
)

# Multi-word phrases to protect from splitting
# These will have spaces replaced with underscores before tokenization
PROTECTED_PHRASES <- list(
  # c("multi word phrase", "multi_word_phrase")
)


# ============================================================
# Uncomment to load reference lists from Google Sheets instead
# ============================================================
# library(gsheet)
#
# excluded_url <- 'https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit#gid=0'
# EXCLUDED_TERMS <- data.frame(gsheet2tbl(excluded_url))
#
# brands_url <- 'https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit#gid=1'
# BRAND_TERMS <- data.frame(gsheet2tbl(brands_url))


# ============================================================
# Load libraries
# ============================================================

library(googleAnalyticsR)
library(stopwords)
library(qdap)


# ============================================================
# Authenticate and get data from Google Analytics
# ============================================================

# Authenticate — opens browser for Google login
ga_auth(new_user = TRUE)

# Find your view ID
account_list <- ga_account_list()
where <- grep(VIEW_NAME_PATTERN, account_list$viewName, ignore.case = TRUE)
ViewID <- account_list[where, ]$viewId

if (length(ViewID) == 0) {
  stop("No view found matching: ", VIEW_NAME_PATTERN,
       "\nAvailable views:\n",
       paste(account_list$viewName, collapse = "\n"))
}

cat("Using view:", account_list$viewName[where], "\n")

# Define filters
# Only include sessions with bounces, clicks, and a matched query
mf  <- met_filter("bounces",   "GREATER_THAN", 0)
mf2 <- met_filter("sessions",  "GREATER",      0)
mf3 <- met_filter("adclicks",  "GREATER",      0)
fc  <- filter_clause_ga4(list(mf, mf2, mf3), operator = "AND")

df  <- dim_filter("admatchedQuery", "EXACT", "(not set)",     not = TRUE)
df2 <- dim_filter("admatchedQuery", "EXACT", "your_brand",    not = TRUE)  # exclude branded queries
fc2 <- filter_clause_ga4(list(df, df2), operator = "AND")

# Pull data from GA
ga_data <- google_analytics(
  viewId      = ViewID,
  date_range  = DATE_RANGE,
  dimensions  = c("adGroup", "admatchedQuery", "keyword"),
  metrics     = c("adClicks", "sessions", "bounces", "adCost"),
  met_filters = fc,
  dim_filters = fc2,
  max         = 200000,
  anti_sample = TRUE
)

cat("Loaded", nrow(ga_data), "queries\n")


# ============================================================
# Preprocessing: protect multi-word phrases
# ============================================================

for (phrase in PROTECTED_PHRASES) {
  ga_data$admatchedQuery <- gsub(
    phrase[1], phrase[2], ga_data$admatchedQuery
  )
}


# ============================================================
# Tokenize: break each query into individual words
# ============================================================

long_data <- NULL
itime <- proc.time()[3]

for (i in 1:nrow(ga_data)) {

  words <- rm_stopwords(ga_data$admatchedQuery[i], " ")[[1]]

  # Remove state names and abbreviations (geographic noise)
  words <- words[!words %in% tolower(state.abb)]
  words <- words[!words %in% tolower(state.name)]

  # Remove keyword terms (words already being targeted)
  words <- words[!words %in% gsub(
    "[+]", "", tolower(strsplit(ga_data$keyword[i], " ")[[1]])
  )]

  # Remove excluded terms
  if (length(EXCLUDED_TERMS) > 0) {
    words <- words[!words %in% tolower(EXCLUDED_TERMS)]
  }

  # Remove brand terms
  words <- words[!words %in% tolower(BRAND_TERMS$term)]

  # Remove English and Spanish stopwords
  words <- words[!words %in% tolower(stopwords(language = "en"))]
  words <- words[!words %in% tolower(stopwords(language = "es"))]

  # Remove numbers
  words <- words[!grepl("[[:digit:]]", words)]

  # Remove punctuation
  words <- gsub("[[:punct:]]", "", words)
  words <- words[words != ""]

  if (length(words) > 0) {
    temp <- data.frame(
      words    = words,
      AdGroup  = ga_data$adGroup[i],
      Query    = ga_data$admatchedQuery[i],
      keyword  = ga_data$keyword[i],
      adClicks = ga_data$adClicks[i],
      sessions = ga_data$sessions[i],
      bounces  = ga_data$bounces[i],
      adCost   = ga_data$adCost[i],
      stringsAsFactors = FALSE
    )
    long_data <- rbind(long_data, temp)
  }

  if (i %% 100 == 0) {
    ctime     <- proc.time()[3]
    timetoend <- ((ctime - itime) / i) * (nrow(ga_data) - i)
    cat(i, "of", nrow(ga_data),
        "| Est. time remaining:", round(timetoend / 60, 1), "min\n")
  }
}

# Remove any remaining rows with numbers in words
long_data <- long_data[!grepl("[[:digit:]]", long_data$words), ]
long_data <- long_data[long_data$words != "", ]

cat("Tokenized into", nrow(long_data), "word-level rows\n")


# ============================================================
# Aggregate to word level
# ============================================================

agg <- aggregate(
  long_data[, c("adClicks", "sessions", "bounces", "adCost")],
  by = list(
    words   = long_data$words,
    AdGroup = long_data$AdGroup,
    query   = long_data$Query,
    keyword = long_data$keyword
  ),
  sum
)

agg$BounceRate <- agg$bounces / agg$sessions
agg <- agg[agg$words != "", ]
agg <- agg[order(agg$BounceRate, decreasing = TRUE), ]

cat("Aggregated to", nrow(agg), "word-level rows\n")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce <- agg[
  agg$BounceRate >= BOUNCE_RATE_THRESHOLD &
  agg$sessions   >= MIN_SESSIONS,
]

cat("Flagged", nrow(high_bounce), "words with bounce rate >=",
    BOUNCE_RATE_THRESHOLD, "and >=", MIN_SESSIONS, "sessions\n")

print(head(high_bounce[, c("words", "sessions", "BounceRate", "adCost")], 10))


# ============================================================
# Save output
# ============================================================

write.csv(agg, OUTPUT_PATH, row.names = FALSE)
cat("Output saved to:", OUTPUT_PATH, "\n")
