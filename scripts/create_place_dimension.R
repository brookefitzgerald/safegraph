library(arrow)
library(dplyr)
library(tidyverse)

BASE_DIR <- "/RSTOR/restricted_data/dewey/"
PLACES_DIR <- str_c(BASE_DIR, "core_geometry_parquet")
SPEND_DIR <- str_c(BASE_DIR, "spend_parquet")

OUTPUT_DIR <- "brookefitzgerald/restricted_data/dewey/processed/dimensions/places"


calculate_month_difference <- function(dfm){
  ## Requires date columns `first` and `last` to be in the data
  
  # Lubridate (for many good reasons) doesn't perform truthy comparisons between 
  # months since months have differing days. Thus the following comparison doesn't return true: 
  #(as_date("2020-01-01") - as_date("2019-01-01"))==dmonths(12)
  
  # This function calculates month differences ignoring the differences in days. 
  dfm <- dfm %>% mutate(
    first_month= month(first),
    first_year = year(first),
    last_month = month(last),
    last_year  = year(last),
    month_difference=case_when(
      # IMPORTANT: open and close the same month counts as 1
      last_year == first_year ~ (last_month - first_month) + 1, 
      
      
      # if opened_on == "2019-12" closed_on=="2020-02", 3;  
      # if opened_on == "2019-12" closed_on=="2021-02", 1+2 +12=15;
      # if opened_on == "2019-02" closed_on=="2020-12", 11 + 12 + 0 = 23;  
      last_year - first_year > 1 ~ (13-first_month) + last_month + (last_year - first_year - 1)*12
    )
  )
}

get_n_months_in_full_spend_dataset <- function(spend_facts){
  # Return the number of months in the full spend dataset (for 4 years will be 48 months)
  return((
    spend_facts %>% 
      summarize(
        first=min(spent_on_date),
        last=max(spent_on_date),
      ) %>% 
      calculate_month_difference() %>%
      rename(n_months_in_panel=month_difference) %>% collect()
  )$n_months_in_panel)
}

aggregate_by_placekey <- function(spend_facts){
  spend_facts %>%
    group_by(placekey) %>%
    summarize(
      n_months_with_spend=n(),
      first = min(spent_on_date),
      last = max(spent_on_date)
    ) %>%
    calculate_month_difference() %>%
    rename(
      first_spent_on=first,
      last_spent_on=last,
      n_months_between_first_and_last_spend=month_difference  # note, will be 1 for only one month of data
    )
}

generate_place_dimension <- function(){
  places_dim  <- open_dataset(PLACES_DIR)
  spend_facts <- open_dataset(SPEND_DIR) %>% 
    mutate(spent_on_date = as_date(str_sub(spend_date_range_start, 1, 10)))
  
  n_months_in_full_spend_dataset <- get_n_months_in_full_spend_dataset(spend_facts)
  
  place_aggregated_spend_facts <- aggregate_by_placekey(spend_facts) %>%
    mutate(
      has_complete_panel=(n_months_with_spend == n_months_in_full_spend_dataset),
      has_consistent_panel=(n_months_between_first_and_last_spend==n_months_with_spend) & (n_months_between_first_and_last_spend > 1) # excluding those with only one month of spend data
    )
  places_dim <- places_dim %>% 
    left_join(place_aggregated_spend_facts, by="placekey") %>%
    mutate(
      n_months_with_spend  = coalesce(n_months_with_spend,  0),
      has_complete_panel   = coalesce(has_complete_panel,   FALSE),
      has_consistent_panel = coalesce(has_consistent_panel, FALSE),
    )

  write_dataset(
    places_dim,
    OUTPUT_DIR,
    format = "parquet",
    partitioning = c("naics_code")
  )
}

generate_place_dimension()