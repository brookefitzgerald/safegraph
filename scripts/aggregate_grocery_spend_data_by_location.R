library(arrow)
library(jsonlite)
library(lubridate)
library(tidyverse)

generate_online_and_transaction_intermediary_dfm <- function(){
  constants <- fromJSON("./safegraph/scripts/constants.json")
  SPEND_DIR <- constants$SPEND_DIR
  PLACES_DIM_DIR <- constants$PLACES_DIM_DIR
  GROCERY_NAICS_CODES <- constants$GROCERY_NAICS_CODES
  
  places <- open_dataset(PLACES_DIM_DIR, partitioning ="naics_code",  format="parquet")
  places_w_spend <- places %>% filter(n_months_with_spend>0 & iso_country_code=="US")
  places_grocery <- places_w_spend %>% filter(naics_code %in% GROCERY_NAICS_CODES)
  
  spend <- open_dataset(SPEND_DIR) %>%
    mutate(spent_on_date = as_date(str_sub(spend_date_range_start, 1, 10)))
  
  
  intermediary_and_online_spend <- spend %>% 
    inner_join(places_grocery, by="placekey") %>%
    mutate(intermediaries=str_replace_all(spend_by_transaction_intermediary, '\"', ''),
           intermediaries=str_replace_all(intermediaries, '\\{', ''),
           intermediaries=str_replace_all(intermediaries, '\\}', '')) %>%
    
    #### I first implemented this in a function, but an R function cannot be converted
    # to base arrow >:'( , so I have to run the functions once for each intermediary
    
    mutate(
      extracted = str_replace_all(intermediaries, str_c(".*No intermediary:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "No intermediary"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_none = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*Instacart:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Instacart"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_instacart = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*DoorDash:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "DoorDash"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_doordash = extracted,    
      
      extracted = str_replace_all(intermediaries, str_c(".*Grubhub:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Grubhub"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_grubhub = extracted, 
      
      extracted = str_replace_all(intermediaries, str_c(".*Postmates:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Postmates"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_postmates = extracted, 
      
      extracted = str_replace_all(intermediaries, str_c(".*OrderUp:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "OrderUp"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_orderup = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*Seamless:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Seamless"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_seamless = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*Yelp:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Yelp"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_yelp = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*Olo:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Olo"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_olo = extracted,
      
      extracted = str_replace_all(intermediaries, str_c(".*Favor:"), ''),
      extracted = str_replace_all(extracted, ",.*", ''),
      extracted = if_else(str_detect(intermediaries, "Favor"), extracted, "0.0"),
      extracted = if_else(str_detect(extracted, "[^0-9\\-\\.]"), 0.0, as.numeric(extracted)),
      int_favor = extracted,
    )%>%
    mutate(all_food_intermediaries= (int_doordash + int_instacart + int_grubhub + int_postmates + int_orderup + int_seamless + int_yelp + int_olo + int_favor),
           all_intermediary_spend=raw_total_spend - int_none,
           full_county_id=str_c(statefp, countyfp))  %>%
    select(contains("int"), raw_num_customers, raw_total_spend, online_spend, region, city, naics_code, has_complete_panel, has_consistent_panel, statefp, countyfp, tractce, geoid, spent_on_date)

  intermediary_and_online_spend %>%
    group_by(full_county_id, spent_on_date) %>% 
    summarize(all_food_intermediaries= sum(all_food_intermediaries),
              all_intermediary_spend = sum(all_intermediary_spend),
              int_doordash           = sum(int_doordash),
              int_instacart          = sum(int_instacart),
              int_grubhub            = sum(int_grubhub),
              int_postmates          = sum(int_postmates),
              int_orderup            = sum(int_orderup),
              int_seamless           = sum(int_seamless),
              int_yelp               = sum(int_yelp),
              int_olo                = sum(int_olo),
              int_favor              = sum(int_favor),
              raw_total_spend        = sum(raw_total_spend),
              online_spend           = sum(online_spend),
              n_places_in_county     = n()
    ) %>%
    collect()
}

generate_daily_spending_dfm <- function(){
  ############## NOT FINISHED ########
  
  constants <- fromJSON("./safegraph/scripts/constants.json")
  SPEND_DIR <- constants$SPEND_DIR
  PLACES_DIM_DIR <- constants$PLACES_DIM_DIR
  GROCERY_NAICS_CODES <- constants$GROCERY_NAICS_CODES
  
  places <- open_dataset(PLACES_DIM_DIR, partitioning ="naics_code",  format="parquet")
  places_w_spend <- places %>% filter(n_months_with_spend>0 & iso_country_code=="US")
  places_grocery <- places_w_spend %>% filter(naics_code %in% GROCERY_NAICS_CODES)
  
  spend <- open_dataset(SPEND_DIR) %>%
    mutate(spent_on_date = as_date(str_sub(spend_date_range_start, 1, 10)))
  
  
  customer_income_and_online_spend <- spend %>% 
    inner_join(places_grocery, by="placekey") %>%
    mutate(
      cbg = geoid,
      
      daily_spending = str_replace_all(spend_by_day, '\\[', ''),
      daily_spending = str_replace_all(daily_spending, '\\]', ''),
      daily_spending = str_replace_all(daily_spending, ' ', ''),
      ###### split days out
      daily_spending = str_split(daily_spending, ",")
    )
}

generate_customer_income_estimates_dfm <- function() {
  constants <- fromJSON("./safegraph/scripts/constants.json")
  SPEND_DIR <- constants$SPEND_DIR
  PLACES_DIM_DIR <- constants$PLACES_DIM_DIR
  GROCERY_NAICS_CODES <- constants$GROCERY_NAICS_CODES
  
  places <- open_dataset(PLACES_DIM_DIR, partitioning ="naics_code",  format="parquet")
  places_w_spend <- places %>% filter(n_months_with_spend>0 & iso_country_code=="US")
  places_grocery <- places_w_spend %>% filter(naics_code %in% GROCERY_NAICS_CODES)
  
  spend <- open_dataset(SPEND_DIR) %>%
    mutate(spent_on_date = as_date(str_sub(spend_date_range_start, 1, 10)))
  
  
  customer_income_and_online_spend <- spend %>% 
    inner_join(places_grocery, by="placekey") %>%
    mutate(
     cbg = geoid,
     
     customer_incomes=str_replace_all(bucketed_customer_incomes, '\"', ''),
     customer_incomes=str_replace_all(customer_incomes, '\\{', ''),
     customer_incomes=str_replace_all(customer_incomes, '\\}', ''),
     
     extracted = str_replace_all(customer_incomes, str_c(".*25-45k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "<25-45k"), extracted, "0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_lt_25_k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*25-45k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "<25-45k"), extracted, "0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_25_to_45k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*45-60k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "45-60k"), extracted, "0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_45_to_60k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*60-75k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "60-75k"), extracted, "0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_60_to_75k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*75-100k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "75-100k"), extracted, "0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_75_to_100k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*100-150k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, "100-150k"), extracted, "0.0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_100_to_150k = extracted,
     
     extracted = str_replace_all(customer_incomes, str_c(".*>150k:"), ''),
     extracted = str_replace_all(extracted, ",.*", ''),
     extracted = if_else(str_detect(customer_incomes, ">150k"), extracted, "0.0"),
     extracted = if_else(str_detect(extracted, "[^0-9]"), 0, as.numeric(extracted)),
     inc_gt_150k = extracted
    ) %>% 
    select(
      starts_with("inc_"), raw_num_customers, raw_total_spend, online_spend, region, city, naics_code, has_complete_panel, has_consistent_panel, statefp, countyfp, tractce, cbg, spent_on_date
    )
  
  customer_income_and_online_spend %>%
    group_by(cbg, spent_on_date) %>% 
    summarize(raw_total_spend        = sum(raw_total_spend),
              raw_num_customers      = sum(raw_num_customers, na.rm=T),
              online_spend           = sum(online_spend),
              n_places_in_county     = n(),
              
              inc_lt_25_k            = sum(inc_lt_25_k, na.rm=T),
              inc_25_to_45k          = sum(inc_25_to_45k, na.rm=T),
              inc_45_to_60k          = sum(inc_45_to_60k, na.rm=T),
              inc_60_to_75k          = sum(inc_60_to_75k, na.rm=T),
              inc_75_to_100k         = sum(inc_75_to_100k, na.rm=T),
              inc_100_to_150k        = sum(inc_100_to_150k, na.rm=T),
              inc_gt_150k            = sum(inc_gt_150k, na.rm=T)
    ) %>% mutate(
      n_customers_w_reported_income = inc_lt_25_k + inc_25_to_45k + inc_45_to_60k + inc_60_to_75k + inc_75_to_100k + inc_100_to_150k + inc_gt_150k,
      est_average_customer_income = (
          (inc_lt_25_k * 20) + 
          (inc_25_to_45k * 35) +
          (inc_45_to_60k * 52.5) +
          (inc_60_to_75k * 67.5) +
          (inc_75_to_100k * 87.5) +
          (inc_100_to_150k * 125) +
          (inc_gt_150k * 175)
      ) / n_customers_w_reported_income
    ) %>%
    collect()
}


time_aggregation <- function(){
  start_time <- Sys.time()
  generate_online_and_transaction_intermediary_dfm()
  end_time <- Sys.time()
  print(end_time - start_time)
  # Time difference of 5.591787 mins
}


