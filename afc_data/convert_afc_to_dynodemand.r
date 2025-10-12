library(arrow)
library(tidyverse)
library(lubridate)

# ==============================================================================
# Script to convert AFC data to dyno-demand format (trip_list.txt)
# ==============================================================================

# Configuration ----------------------------------------------------------------
INPUT_FILE <- "d:/fast-trips/afc_data/sample_data/2025_H1_afc_data_endstop_imputed.parquet"
OUTPUT_FILE <- "d:/fast-trips/afc_data/trip_list.txt"

# Filter for a specific date (YYYY-MM-DD format)
TARGET_DATE <- "2025-04-10"  # Change this to your desired date

# Default values for fields not in AFC data
DEFAULT_MODE <- "walk-local_bus-walk"  # Default transit mode
DEFAULT_PURPOSE <- "other"              # Default trip purpose
DEFAULT_VOT <- 15.0                     # Value of time in $/hour
DEFAULT_TIME_TARGET <- "departure"      # arrival or departure

# ==============================================================================
# Read and filter AFC data
# ==============================================================================

cat("Reading AFC data from:", INPUT_FILE, "\n")
afc_data <- read_parquet(INPUT_FILE)

# Display column names for reference
cat("\nAvailable columns in AFC data:\n")
print(names(afc_data))
cat("\nFirst few rows:\n")
print(head(afc_data, 3))

# ==============================================================================
# IMPORTANT: This script aggregates AFC legs into complete journeys
# ==============================================================================
# One journey may have multiple legs (with transfers)
# Script groups by journey_id to create one trip per journey:
# - Origin: First leg's boarding station
# - Destination: Last leg's alighting station
# - Departure: First leg's boarding time
# - Arrival: Last leg's alighting time (or departure + 50 min if missing)

# Filter for target date - extract date from start_time
afc_filtered <- afc_data %>%
  mutate(trip_date = as.Date(ymd_hms(start_time))) %>%
  filter(trip_date == as.Date(TARGET_DATE))

cat("\nFiltered to", nrow(afc_filtered), "records for date:", TARGET_DATE, "\n")

# ==============================================================================
# Convert to dyno-demand format
# ==============================================================================

trip_list <- afc_filtered %>%
  # Sort by user, journey and leg to ensure proper ordering
  arrange(uid, journey_id, leg_id) %>%

  # Group by user AND journey to aggregate legs into complete journeys
  # (journey_id is only unique within each uid)
  group_by(uid, journey_id) %>%
  summarize(
    # Get first leg (origin leg)
    first_start_time = first(start_time),
    first_start_station_no = first(start_station_no),
    first_start_station = first(start_station),
    first_route = first(route),

    # Get last leg (destination leg)
    last_end_time = last(end_time),
    last_end_station_no = last(end_station_no),
    last_end_station = last(end_station),
    last_start_station_no = last(start_station_no),  # fallback if end station missing

    # Keep for reference
    num_legs = n(),
    .groups = "drop"
  ) %>%

  # Create sequential trip IDs (globally unique since all person_id=0)
  arrange(first_start_time) %>%
  mutate(person_trip_id = row_number()) %>%

  # Map AFC fields to dyno-demand fields
  transmute(
    # Required fields for dyno-demand
    # Use 0 for person_id to identify trips without disaggregate person records
    person_id = 0,
    person_trip_id = as.integer(person_trip_id),

    # Origin TAZ - use first leg's boarding station
    o_taz = if_else(!is.na(first_start_station_no),
                    as.character(as.integer(first_start_station_no)),
                    as.character(first_start_station)),

    # Destination TAZ - use last leg's alighting station (or last boarding if missing)
    d_taz = if_else(!is.na(last_end_station_no) & last_end_station_no > 0,
                    as.character(as.integer(last_end_station_no)),
                    if_else(last_end_station != "" & !is.na(last_end_station),
                            as.character(last_end_station),
                            as.character(as.integer(last_start_station_no)))),

    # Mode - format: access_mode-transit_mode-egress_mode
    # Infer from first leg's route
    mode = case_when(
      !is.na(first_route) & str_detect(tolower(first_route), "rail|metro|train") ~ "walk-commuter_rail-walk",
      !is.na(first_route) & str_detect(tolower(first_route), "express|rapid") ~ "walk-premium_bus-walk",
      !is.na(first_route) ~ "walk-local_bus-walk",
      TRUE ~ DEFAULT_MODE
    ),

    # Purpose - infer from journey departure time
    purpose = case_when(
      hour(ymd_hms(first_start_time)) >= 6 & hour(ymd_hms(first_start_time)) <= 9 ~ "work",
      hour(ymd_hms(first_start_time)) >= 16 & hour(ymd_hms(first_start_time)) <= 19 ~ "work",
      TRUE ~ DEFAULT_PURPOSE
    ),

    # Departure time - first leg's start time
    departure_time = format(ymd_hms(first_start_time), "%H:%M:%S"),

    # Arrival time - last leg's end time, or journey start + 50 minutes if missing
    arrival_time = if_else(
      !is.na(last_end_time) & last_end_time != "",
      format(ymd_hms(last_end_time), "%H:%M:%S"),
      format(ymd_hms(first_start_time) + minutes(50), "%H:%M:%S")
    ),

    # Time target - whether arrival or departure time is more important
    time_target = DEFAULT_TIME_TARGET,

    # Value of time in dollars/hour
    vot = DEFAULT_VOT
  ) %>%

  # Remove only rows with missing origin or destination (keep even if end_time was missing)
  filter(!is.na(o_taz), !is.na(d_taz), o_taz != "", d_taz != "")

# ==============================================================================
# Quality checks
# ==============================================================================

cat("\n=== Quality Check ===\n")
cat("Total journeys converted to trips:", nrow(trip_list), "\n")
cat("Date range of departure times:",
    min(trip_list$departure_time), "to", max(trip_list$departure_time), "\n")

# Check for any invalid values
cat("\nChecking for missing values:\n")
print(colSums(is.na(trip_list)))

# Count journeys with estimated arrival times
cat("\nNote: All journeys retained, including those with missing end times\n")
cat("(Missing arrival times estimated as departure + 50 minutes)\n")

# Show sample of output
cat("\n=== Sample Output ===\n")
print(head(trip_list, 10))

# ==============================================================================
# Write output
# ==============================================================================

cat("\nWriting output to:", OUTPUT_FILE, "\n")
write_csv(trip_list, OUTPUT_FILE)

cat("\n=== Conversion Complete ===\n")
cat("Output file:", OUTPUT_FILE, "\n")
cat("Total journeys written as trips:", nrow(trip_list), "\n")

# ==============================================================================
# Summary statistics
# ==============================================================================

cat("\n=== Summary Statistics ===\n")
cat("Journeys by mode:\n")
print(table(trip_list$mode))
cat("\nJourneys by purpose:\n")
print(table(trip_list$purpose))
cat("\nJourneys by time target:\n")
print(table(trip_list$time_target))
cat("\n")
