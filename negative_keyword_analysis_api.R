# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# R — GA4 Data API Version (No Manual Export Required)
# ============================================================
# Author: Elizabeth Lawson
# Description:
#   Identifies potential negative keywords by breaking paid search
#   queries into individual words and aggregating GA4 post-click
#   behavioral metrics (bounce rate, sessions, cost) at the word level.
#
#   This version pulls data directly from GA4 using the googleAnalyticsR
#   package. No manual export or Looker Studio access required.
#
# Requirements:
#   install.packages(c("googleAnalyticsR", "readr"))
#
# Setup:
#   Authentication Option A — OAuth (browser-based, easiest):
#     Run ga_auth() and log in with your Google account.
#     Your account must have at least Viewer access to the GA4 property.
#
#   Authentication Option B — Service Account JSON key:
#     1. Create a service account in Google Cloud Console
#        https://console.cloud.google.com/iam-admin/serviceaccounts
#     2. Download the JSON key file
#     3. Grant the service account Viewer access to your GA4 property
#     4. Set KEY_FILE_PATH below and set USE_SERVICE_ACCOUNT <- TRUE
#
# Note:
#   The GA4 Data API does not support the Google Analytics demo account.
#   Use negative_keyword_analysis_manual.R with sample_data.csv
#   to test with demo data.
#
# For the manual export version (no API required) see:
#   negative_keyword_analysis_manual.R
# ============================================================

library(googleAnalyticsR)
library(readr)

# ============================================================
# Configuration
# ============================================================

# Authentication method
# TRUE  = service account JSON key (fully automated, no browser needed)
# FALSE = OAuth browser login (easier setup)
USE_SERVICE_ACCOUNT <- FALSE

# Path to service account JSON key (only used if USE_SERVICE_ACCOUNT = TRUE)
KEY_FILE_PATH <- "your-service-account-key.json"

# Your GA4 property ID (numeric, found in GA4 Admin → Property Settings)
GA4_PROPERTY_ID <- "your-ga4-property-id"

# Date range for analysis
START_DATE <- "90daysAgo"
END_DATE   <- "today"

# Maximum rows to pull from GA4
MAX_ROWS <- 10000

# Column names (do not change)
QUERY_COLUMN    <- "Session Google Ads query"
SESSIONS_COLUMN <- "Sessions"
ENGAGED_COLUMN  <- "Engaged sessions"
COST_COLUMN     <- "Ads cost"

# ============================================================
# Brand terms
# Replace with your own brand name(s) and all variations.
# ============================================================
BRAND_TERMS <- c(
  "your_brand_name"           # Replace with your actual brand name
  # "your_brand_abbreviation",
  # "common_misspelling"
)

# ============================================================
# Protected phrases
# Multi-word terms to treat as a single token.
# ============================================================
PROTECTED_PHRASES <- c(
  # "myasthenia gravis" = "myasthenia_gravis",
  # "rolls royce"       = "rolls_royce"
)

# Thresholds
MIN_SESSIONS          <- 3
BOUNCE_RATE_THRESHOLD <- 0.50

# Output files
OUTPUT_WORDS_PATH   <- "high_bounce_words.csv"
OUTPUT_QUERIES_PATH <- "flagged_queries.csv"


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
# Pull data from GA4 Data API
# ============================================================

cat("Pulling data from GA4 property", GA4_PROPERTY_ID, "...\n")

df <- ga_data(
  propertyId = GA4_PROPERTY_ID,
  metrics    = c("sessions", "engagedSessions", "advertiserAdCost"),
  dimensions = c("sessionGoogleAdsQuery"),
  date_range = c(START_DATE, END_DATE),
  limit      = MAX_ROWS
)

# Rename columns to match standard format
names(df)[names(df) == "sessionGoogleAdsQuery"] <- QUERY_COLUMN
names(df)[names(df) == "sessions"]              <- SESSIONS_COLUMN
names(df)[names(df) == "engagedSessions"]       <- ENGAGED_COLUMN
names(df)[names(df) == "advertiserAdCost"]      <- COST_COLUMN

# Remove rows with no query or zero sessions
df <- df[!is.na(df[[QUERY_COLUMN]]), ]
df <- df[df[[QUERY_COLUMN]] != "(not set)", ]
df <- df[df[[SESSIONS_COLUMN]] > 0, ]

cat("Pulled", nrow(df), "queries from GA4\n")


# ============================================================
# Prepare data
# ============================================================

if (length(PROTECTED_PHRASES) > 0) {
  for (i in seq_along(PROTECTED_PHRASES)) {
    df[[QUERY_COLUMN]] <- gsub(
      names(PROTECTED_PHRASES)[i],
      PROTECTED_PHRASES[i],
      df[[QUERY_COLUMN]],
      ignore.case = TRUE
    )
  }
}

df$Bounces     <- df[[SESSIONS_COLUMN]] - df[[ENGAGED_COLUMN]]
df$Bounce_Rate <- df$Bounces / df[[SESSIONS_COLUMN]]

cat("Loaded", nrow(df), "queries after filtering\n")


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
      Query       = as.character(df[[QUERY_COLUMN]][i]),
      Word        = words,
      Cost        = df[[COST_COLUMN]][i],
      Sessions    = df[[SESSIONS_COLUMN]][i],
      Bounces     = df$Bounces[i],
      Bounce_Rate = df$Bounce_Rate[i],
      stringsAsFactors = FALSE
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

names(long_data)[names(long_data) == "Cost"]     <- COST_COLUMN
names(long_data)[names(long_data) == "Sessions"] <- SESSIONS_COLUMN
long_data <- long_data[long_data[[SESSIONS_COLUMN]] > 0, ]

cat("Tokenized into", nrow(long_data), "word-level rows\n")


# ============================================================
# Aggregate to word level
# ============================================================

word_data <- aggregate(
  long_data[, c(COST_COLUMN, SESSIONS_COLUMN, "Bounces")],
  by  = list(Word = long_data$Word),
  FUN = sum
)

query_counts          <- aggregate(Query ~ Word, data = long_data,
                                    FUN = function(x) length(unique(x)))
names(query_counts)[2] <- "Query_Count"
word_data              <- merge(word_data, query_counts, by = "Word")
word_data$Bounce_Rate  <- word_data$Bounces / word_data[[SESSIONS_COLUMN]]
word_data              <- word_data[word_data[[SESSIONS_COLUMN]] > 0, ]
word_data              <- word_data[order(word_data$Bounce_Rate,
                                           word_data[[SESSIONS_COLUMN]],
                                           decreasing = TRUE), ]

cat("Aggregated to", nrow(word_data), "unique words\n")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce <- word_data[
  word_data$Bounce_Rate        >= BOUNCE_RATE_THRESHOLD &
  word_data[[SESSIONS_COLUMN]] >= MIN_SESSIONS,
]

cat("\nFlagged", nrow(high_bounce), "words:\n")
print(high_bounce)

write_csv(high_bounce, OUTPUT_WORDS_PATH)
cat("Saved to:", OUTPUT_WORDS_PATH, "\n")
cat("Load this file into Google Sheets to connect to your Looker Studio dashboard.\n")

flagged_words   <- tolower(high_bounce$Word)
flagged_queries <- long_data[tolower(long_data$Word) %in% flagged_words, ]
flagged_queries <- flagged_queries[order(flagged_queries[[SESSIONS_COLUMN]],
                                          flagged_queries$Bounce_Rate,
                                          decreasing = TRUE), ]
write_csv(flagged_queries, OUTPUT_QUERIES_PATH)
cat("Query detail saved to:", OUTPUT_QUERIES_PATH, "\n")


# ============================================================
# Helper: investigate a specific word
# ============================================================

investigate_word <- function(word) {
  subset <- long_data[tolower(long_data$Word) == tolower(word), ]
  if (nrow(subset) == 0) {
    cat("No queries found containing '", word, "'\n", sep = "")
    return(invisible(NULL))
  }
  cat("\n=== '", word, "' ===\n", sep = "")
  cat("Sessions:    ", sum(subset[[SESSIONS_COLUMN]]), "\n")
  cat("Bounce rate: ",
      round(sum(subset$Bounces) / sum(subset[[SESSIONS_COLUMN]]) * 100, 1),
      "%\n", sep = "")
  cat("Cost:        $", round(sum(subset[[COST_COLUMN]]), 2), "\n\n")
  print(subset[order(subset[[SESSIONS_COLUMN]], decreasing = TRUE),
               c("Query", SESSIONS_COLUMN, "Bounce_Rate", COST_COLUMN)])
}

# investigate_word("garage")
# investigate_word("pixel")
