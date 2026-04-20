# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# R — GA4 Data API Version (No Manual Export Required)
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Pulls data directly from GA4 using googleAnalyticsR and identifies
#   potential negative keywords using word-level post-click analysis.
#
# Requirements:
#   install.packages(c("googleAnalyticsR", "readr"))
#
# Setup:
#   Authentication Option A — OAuth (browser-based, easiest):
#     Set USE_SERVICE_ACCOUNT <- FALSE and run ga_auth()
#
#   Authentication Option B — Service Account JSON key (fully automated):
#     1. Create a service account in Google Cloud Console
#     2. Download the JSON key file
#     3. Grant Viewer access to your GA4 property
#     4. Set USE_SERVICE_ACCOUNT <- TRUE and update KEY_FILE_PATH
#
# Output:
#   high_bounce_words.csv  — word-level summary with flagged words
#   flagged_queries.csv    — query-level detail for flagged words
#
#   Both files use standardized column names matching the BigQuery
#   SQL version so all implementations connect to the same
#   Looker Studio dashboard without any field remapping.
#
# Note:
#   The GA4 Data API does not support the Google Analytics demo account.
#   Use negative_keyword_analysis_manual.R with sample_data.csv
#   to test with demo data.
# ============================================================

library(googleAnalyticsR)
library(readr)

# ============================================================
# Configuration
# ============================================================

USE_SERVICE_ACCOUNT <- FALSE
KEY_FILE_PATH       <- "your-service-account-key.json"
GA4_PROPERTY_ID     <- "your-ga4-property-id"
START_DATE          <- "90daysAgo"
END_DATE            <- "today"
MAX_ROWS            <- 10000

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

MIN_SESSIONS          <- 3
BOUNCE_RATE_THRESHOLD <- 0.50
OUTPUT_WORDS_PATH     <- "high_bounce_words.csv"
OUTPUT_QUERIES_PATH   <- "flagged_queries.csv"


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
# Authenticate
# ============================================================

if (USE_SERVICE_ACCOUNT) {
  cat("Authenticating with service account...\n")
  ga_auth(json_file = KEY_FILE_PATH)
} else {
  cat("Authenticating with OAuth (browser will open)...\n")
  ga_auth()
}


# ============================================================
# Pull data from GA4
# ============================================================

cat("Pulling data from GA4 property", GA4_PROPERTY_ID, "...\n")

raw <- ga_data(
  propertyId = GA4_PROPERTY_ID,
  metrics    = c("sessions", "engagedSessions", "advertiserAdCost"),
  dimensions = c("sessionGoogleAdsQuery"),
  date_range = c(START_DATE, END_DATE),
  limit      = MAX_ROWS
)

# Rename to standard column names
df <- data.frame(
  session_google_ads_query = raw$sessionGoogleAdsQuery,
  sessions                 = raw$sessions,
  engaged_sessions         = raw$engagedSessions,
  advertiser_ad_cost       = raw$advertiserAdCost,
  stringsAsFactors         = FALSE
)

df <- df[!is.na(df$session_google_ads_query), ]
df <- df[df$session_google_ads_query != "(not set)", ]
df <- df[df$sessions > 0, ]

cat("Pulled", nrow(df), "queries from GA4\n")


# ============================================================
# Prepare data
# ============================================================

if (length(PROTECTED_PHRASES) > 0) {
  for (i in seq_along(PROTECTED_PHRASES)) {
    df$session_google_ads_query <- gsub(
      names(PROTECTED_PHRASES)[i],
      PROTECTED_PHRASES[i],
      df$session_google_ads_query, ignore.case = TRUE
    )
  }
}

df$bounces     <- df$sessions - df$engaged_sessions
df$bounce_rate <- df$bounces / df$sessions


# ============================================================
# Tokenize
# ============================================================

long_data <- NULL
itime     <- proc.time()[3]

for (i in 1:nrow(df)) {
  query <- tolower(as.character(df$session_google_ads_query[i]))
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
      session_google_ads_query = as.character(df$session_google_ads_query[i]),
      word                     = words,
      advertiser_ad_cost       = df$advertiser_ad_cost[i],
      sessions                 = df$sessions[i],
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

word_data <- aggregate(
  long_data[, c("advertiser_ad_cost", "sessions", "bounces")],
  by  = list(word = long_data$word),
  FUN = sum
)

query_counts           <- aggregate(
  session_google_ads_query ~ word,
  data = long_data,
  FUN  = function(x) length(unique(x))
)
names(query_counts)[2] <- "query_count"
word_data              <- merge(word_data, query_counts, by = "word")
word_data$bounce_rate  <- word_data$bounces / word_data$sessions
word_data              <- word_data[word_data$sessions > 0, ]
word_data              <- word_data[order(word_data$bounce_rate,
                                           word_data$sessions,
                                           decreasing = TRUE), ]

cat("Aggregated to", nrow(word_data), "unique words\n")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce <- word_data[
  word_data$bounce_rate >= BOUNCE_RATE_THRESHOLD &
  word_data$sessions    >= MIN_SESSIONS,
]

cat("\nFlagged", nrow(high_bounce), "words:\n")
print(high_bounce)

write_csv(high_bounce, OUTPUT_WORDS_PATH)
cat("Saved to:", OUTPUT_WORDS_PATH, "\n")
cat("Load this file into Google Sheets to connect to your Looker Studio dashboard.\n")

flagged_words   <- tolower(high_bounce$word)
flagged_queries <- long_data[tolower(long_data$word) %in% flagged_words, ]
flagged_queries <- flagged_queries[order(flagged_queries$sessions,
                                          flagged_queries$bounce_rate,
                                          decreasing = TRUE), ]
write_csv(flagged_queries, OUTPUT_QUERIES_PATH)
cat("Query detail saved to:", OUTPUT_QUERIES_PATH, "\n")


# ============================================================
# Helper: investigate a specific word
# ============================================================

investigate_word <- function(word) {
  subset <- long_data[tolower(long_data$word) == tolower(word), ]
  if (nrow(subset) == 0) {
    cat("No queries found containing '", word, "'\n", sep = "")
    return(invisible(NULL))
  }
  cat("\n=== '", word, "' ===\n", sep = "")
  cat("Sessions:    ", sum(subset$sessions), "\n")
  cat("Bounce rate: ",
      round(sum(subset$bounces) / sum(subset$sessions) * 100, 1), "%\n", sep = "")
  cat("Cost:        $", round(sum(subset$advertiser_ad_cost), 2), "\n\n")
  print(subset[order(subset$sessions, decreasing = TRUE),
               c("session_google_ads_query", "sessions",
                 "bounce_rate", "advertiser_ad_cost")])
}

# investigate_word("garage")
# investigate_word("pixel")
