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
# Usage:
#   1. Update DATA_PATH to point to your exported CSV
#   2. Update BRAND_TERMS with your brand name(s)
#   3. Adjust thresholds as needed
#   4. Run the script
#   5. Load high_bounce_words.csv into Google Sheets
#   6. Connect your Looker Studio dashboard to that Google Sheet
#
# For the GA4 API version (no manual export required) see:
#   negative_keyword_analysis_api.R
# ============================================================

library(readr)

# ============================================================
# Configuration
# ============================================================

DATA_PATH <- "sample_data.csv"

# Column names
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
# Format: c("original phrase" = "replacement_with_underscores")
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
# Load and prepare data
# ============================================================

df <- read_csv(DATA_PATH, skip = SKIP_ROWS, show_col_types = FALSE)
df <- df[!is.na(df[[QUERY_COLUMN]]), ]

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

df$Bounces      <- df[[SESSIONS_COLUMN]] - df[[ENGAGED_COLUMN]]
df$Bounce_Rate  <- df$Bounces / df[[SESSIONS_COLUMN]]

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

query_counts        <- aggregate(Query ~ Word, data = long_data,
                                  FUN = function(x) length(unique(x)))
names(query_counts)[2] <- "Query_Count"
word_data           <- merge(word_data, query_counts, by = "Word")
word_data$Bounce_Rate <- word_data$Bounces / word_data[[SESSIONS_COLUMN]]
word_data           <- word_data[word_data[[SESSIONS_COLUMN]] > 0, ]
word_data           <- word_data[order(word_data$Bounce_Rate,
                                        word_data[[SESSIONS_COLUMN]],
                                        decreasing = TRUE), ]

cat("Aggregated to", nrow(word_data), "unique words\n")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce <- word_data[
  word_data$Bounce_Rate      >= BOUNCE_RATE_THRESHOLD &
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
