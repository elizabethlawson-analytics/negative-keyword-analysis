# ============================================================
# Negative Keyword Analysis: Word-Level Post-Click Behavior
# R Implementation
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
#   install.packages("readr")
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

library(readr)

# ============================================================
# Configuration — update these values for your use case
# ============================================================

# Path to your Looker Studio or GA4 export CSV
DATA_PATH <- "sample_data.csv"

# Column names — update if your export uses different names
QUERY_COLUMN    <- "Session Google Ads query"  # Looker Studio export format
SESSIONS_COLUMN <- "Sessions"
ENGAGED_COLUMN  <- "Engaged sessions"
COST_COLUMN     <- "Ads cost"                  # Use 'Google Ads cost' for GA4 direct export

# Number of header rows to skip
# Looker Studio exports: 0
# Raw GA4 exports: typically 6
SKIP_ROWS <- 0

# ============================================================
# Brand terms
# Replace these with your own brand name(s).
# Words in this list will be removed from queries before analysis
# so that branded terms don't distort the word-level results.
# Add all variations: full name, abbreviations, common misspellings.
# ============================================================
BRAND_TERMS <- c(
  "google"      # Replace with your brand name
  # Add more variations:
  # "your brand abbreviation",
  # "common misspelling"
)

# ============================================================
# Multi-word phrases to protect from splitting
# Add phrases that should be treated as a single token.
# Format: c("original phrase" = "replacement_with_underscores")
# ============================================================
PROTECTED_PHRASES <- c(
  # Healthcare examples:
  # "myasthenia gravis" = "myasthenia_gravis",
  # "lambert eaton"     = "lambert_eaton",
  # Automotive examples:
  # "rolls royce"       = "rolls_royce",
  # "alfa romeo"        = "alfa_romeo"
  # Add your own:
)

# Minimum sessions for a word to be included in output
MIN_SESSIONS <- 3

# Bounce rate threshold for flagging words as potential negatives
BOUNCE_RATE_THRESHOLD <- 0.50

# Output file paths
OUTPUT_WORDS_PATH   <- "high_bounce_words.csv"
OUTPUT_QUERIES_PATH <- "flagged_queries.csv"


# ============================================================
# Stopwords
# Covers English, Spanish, US geography, and common search terms.
# Add or remove based on your industry and use case.
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
  "por","con","no","una","su","para","como","más","pero",
  "sus","le","ya","o","porque","cuando","muy","sin","sobre",
  "también","me","hasta","hay","donde","quien","desde","todo",
  "nos","durante","estados","todo","eso","las","mi","del",
  "se","lo","le","da","si","al","e"
)

# US state names and abbreviations
# Removes geographic modifiers common in local search campaigns
US_STATES <- c(
  "alabama","alaska","arizona","arkansas","california","colorado",
  "connecticut","delaware","florida","georgia","hawaii","idaho",
  "illinois","indiana","iowa","kansas","kentucky","louisiana",
  "maine","maryland","massachusetts","michigan","minnesota",
  "mississippi","missouri","montana","nebraska","nevada",
  "hampshire","jersey","mexico","york","carolina","dakota",
  "ohio","oklahoma","oregon","pennsylvania","rhode","island",
  "tennessee","texas","utah","vermont","virginia","washington",
  "virginia","wisconsin","wyoming",
  # Abbreviations
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

# Remove rows with no query data
df <- df[!is.na(df[[QUERY_COLUMN]]), ]

# Apply protected phrase replacements before tokenizing
if (length(PROTECTED_PHRASES) > 0) {
  for (i in seq_along(PROTECTED_PHRASES)) {
    phrase      <- names(PROTECTED_PHRASES)[i]
    replacement <- PROTECTED_PHRASES[i]
    df[[QUERY_COLUMN]] <- gsub(phrase, replacement,
                                df[[QUERY_COLUMN]], ignore.case = TRUE)
  }
}

# Calculate bounces and bounce rate
df$Bounces      <- df[[SESSIONS_COLUMN]] - df[[ENGAGED_COLUMN]]
df$`Bounce Rate` <- df$Bounces / df[[SESSIONS_COLUMN]]

cat("Loaded", nrow(df), "queries\n")
print(head(df))


# ============================================================
# Tokenize: break each query into individual words
# ============================================================

long_data <- NULL
itime     <- proc.time()[3]

for (i in 1:nrow(df)) {

  query <- tolower(as.character(df[[QUERY_COLUMN]][i]))

  # Split into words
  words <- unlist(strsplit(query, " "))

  # Clean punctuation from each word
  words <- gsub("[.,!?()\\[\\]\"'\\-]", "", words)

  # Filter: remove stopwords, brand terms, numbers, short words
  words <- words[
    !words %in% ALL_STOPWORDS &          # not a stopword
    !words %in% BRAND_TERMS &            # not a brand term
    !grepl("[[:digit:]]", words) &       # contains no digits
    nchar(words) > 1 &                   # more than 1 character
    words != ""                          # not empty
  ]

  if (length(words) > 0) {
    temp <- data.frame(
      Query        = as.character(df[[QUERY_COLUMN]][i]),
      Word         = words,
      Cost         = df[[COST_COLUMN]][i],
      Sessions     = df[[SESSIONS_COLUMN]][i],
      Bounces      = df$Bounces[i],
      Bounce_Rate  = df$`Bounce Rate`[i],
      stringsAsFactors = FALSE
    )
    long_data <- rbind(long_data, temp)
  }

  if (i %% 100 == 0) {
    ctime     <- proc.time()[3]
    timetoend <- ((ctime - itime) / i) * (nrow(df) - i)
    cat(i, "of", nrow(df),
        "| Est. time remaining:", round(timetoend / 60, 1), "min\n")
  }
}

# Rename columns to match config
names(long_data)[names(long_data) == "Cost"] <- COST_COLUMN
names(long_data)[names(long_data) == "Sessions"] <- SESSIONS_COLUMN

# Filter out zero-session rows
long_data <- long_data[long_data[[SESSIONS_COLUMN]] > 0, ]

cat("Tokenized into", nrow(long_data), "word-level rows\n")


# ============================================================
# Aggregate to word level
# ============================================================

word_data <- aggregate(
  long_data[, c(COST_COLUMN, SESSIONS_COLUMN, "Bounces")],
  by   = list(Word = long_data$Word),
  FUN  = sum
)

# Add query count
query_counts <- aggregate(
  Query ~ Word,
  data = long_data,
  FUN  = function(x) length(unique(x))
)
names(query_counts)[2] <- "Query_Count"
word_data <- merge(word_data, query_counts, by = "Word")

word_data$`Bounce Rate` <- word_data$Bounces / word_data[[SESSIONS_COLUMN]]
word_data <- word_data[word_data[[SESSIONS_COLUMN]] > 0, ]
word_data <- word_data[order(word_data$`Bounce Rate`,
                              word_data[[SESSIONS_COLUMN]],
                              decreasing = TRUE), ]

cat("Aggregated to", nrow(word_data), "unique words\n")


# ============================================================
# Flag high-bounce words
# ============================================================

high_bounce <- word_data[
  word_data$`Bounce Rate` >= BOUNCE_RATE_THRESHOLD &
  word_data[[SESSIONS_COLUMN]] >= MIN_SESSIONS,
]

cat("\nFlagged", nrow(high_bounce), "words with bounce rate >=",
    BOUNCE_RATE_THRESHOLD, "and >=", MIN_SESSIONS, "sessions:\n")
print(high_bounce)

write_csv(high_bounce, OUTPUT_WORDS_PATH)
cat("\nWord-level output saved to:", OUTPUT_WORDS_PATH, "\n")
cat("Load this file into Google Sheets to connect to your Looker Studio dashboard.\n")


# ============================================================
# Drill into flagged words — show associated queries
# ============================================================

flagged_words <- tolower(high_bounce$Word)

flagged_queries <- long_data[tolower(long_data$Word) %in% flagged_words, ]
flagged_queries <- flagged_queries[
  order(flagged_queries[[SESSIONS_COLUMN]],
        flagged_queries$Bounce_Rate,
        decreasing = TRUE), ]

write_csv(flagged_queries, OUTPUT_QUERIES_PATH)
cat("Query-level detail saved to:", OUTPUT_QUERIES_PATH, "\n")


# ============================================================
# Helper: investigate a specific word
# ============================================================

investigate_word <- function(word) {
  #' Print all queries containing a specific flagged word,
  #' along with their sessions, bounce rate, and cost.
  #'
  #' Usage: investigate_word("pixel")
  #'        investigate_word("garage")

  subset <- long_data[tolower(long_data$Word) == tolower(word), ]

  if (nrow(subset) == 0) {
    cat("No queries found containing '", word, "'\n", sep = "")
    return(invisible(NULL))
  }

  cat("\n=== Queries containing '", word, "' ===\n", sep = "")
  cat("Total sessions:      ", sum(subset[[SESSIONS_COLUMN]]), "\n")
  cat("Overall bounce rate: ",
      round(sum(subset$Bounces) / sum(subset[[SESSIONS_COLUMN]]) * 100, 1),
      "%\n", sep = "")
  cat("Total cost:          $",
      round(sum(subset[[COST_COLUMN]]), 2), "\n\n", sep = "")
  cat("Individual queries:\n")

  result <- subset[, c("Query", SESSIONS_COLUMN, "Bounce_Rate", COST_COLUMN)]
  result <- result[order(result[[SESSIONS_COLUMN]], decreasing = TRUE), ]
  print(result)
}

# Example usage — uncomment to investigate specific words:
# investigate_word("pixel")
# investigate_word("garage")
# investigate_word("usa")
