################################################################################
# This codes downloads smoke plume data from NOAA URL and then processes it.   # 
# Some files had to be downloaded multiple times, and some were malformed or   #
# needed special treatment. It returns the shapefiles at the census block group#
# level where there is any intersection (not coverage!) of the smoke plume and #
# the CBG.                                                                     #
################################################################################
library(tibble)
library(dplyr)
library(arrow)
library(sf)
library(glue)
library(tigris)
library(data.table, include.only = CJ)
library(stringr)

# Removing the US colonies Guam, the Virgin Islands, the Northern Mariana Islands, Puerto Rico, and American Samoa

us_colonies <- c("GU", "VI", "MP", "PR", "AS")
us_colony_codes <- c(60, 72, 78, 66, 69)
us_block_groups <- block_groups(cb=T) %>% 
  filter(!STATEFP %in% us_colony_codes) %>%
  st_transform(st_crs("EPSG:4326"))

all_dates <- seq(as.Date("2019-01-01"), as.Date("2022-12-31"), by="days")

pb = txtProgressBar(min = 0, max = length(all_dates), initial = 0) 

all_smoke_plume_cbgs <- data.frame()
error_days <- c(all_dates[191])
error_messages <- c("cannot open URL 'https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/2019/07/hms_smoke20190710.zip': HTTP status was '404 Not Found'")

for(i in 1:length(all_dates)){
  each_day <- all_dates[[i]]
  yr    <- as.character(year(each_day))
  mnth  <- str_pad(month(each_day), width=2, pad="0", side="left")
  day   <- str_pad(mday(each_day), width=2, pad="0", side="left")
  tryCatch({
    if(!file.exists("~/plume_shpfiles/unzipped/hms_smoke{yr}{mnth}{day}.shp")){
      zipped_fp <- glue("~/plume_shpfiles/zipped/hms_smoke{yr}{mnth}{day}.zip")
      download.file(glue("https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/{yr}/{mnth}/hms_smoke{yr}{mnth}{day}.zip"), destfile=zipped_fp, quiet=T)
      unzip(glue("~/plume_shpfiles/zipped/hms_smoke{yr}{mnth}{day}.zip"), exdir="~/plume_shpfiles/unzipped/")
      file.remove(zipped_fp)
    }
    daily_smoke_plumes <- st_make_valid(st_read(glue("/RSTOR/bfitzgerald/plume_shpfiles/unzipped/hms_smoke{yr}{mnth}{day}.shp"), quiet=T))
    cbgs_w_smoke_on_day <- st_join(us_block_groups, daily_smoke_plumes, st_intersects, left=F) %>% 
      st_set_geometry(NULL) %>%
      select(AFFGEOID, Density) %>%
      mutate(density=factor(Density, levels=c("Light", "Medium", "Heavy"), ordered=T)) %>%
      rename(census_block_group=AFFGEOID) %>% 
      group_by(census_block_group) %>%
      summarize(density=max(density)) %>% 
      mutate(smoke_month=str_c(yr, mnth), smoke_day=str_c(smoke_month, day))
    all_smoke_plume_cbgs <- bind_rows(all_smoke_plume_cbgs, cbgs_w_smoke_on_day)
  }, error=function(e){
    error_days <- c(error_days, all_dates[i])
    error_messages <- c(error_messages, conditionMessage(e))
    NULL
  })
  
  setTxtProgressBar(pb,i)
}

## Manually fixing the ones that the loop missed:
missing_dates <- all_dates[!all_dates %in% as.Date(unique(all_smoke_plume_cbgs$smoke_day), format='%Y%m%d')]
for(i in 1:length(missing_dates)){
  each_day <- missing_dates[[i]]
  yr    <- as.character(year(each_day))
  mnth  <- str_pad(month(each_day), width=2, pad="0", side="left")
  day   <- str_pad(mday(each_day), width=2, pad="0", side="left")
  # 2019-07-10 is missing the data from the HOA website
  if(each_day !=as.Date("2019-07-10")){
    tryCatch({
      daily_smoke_plumes <- st_make_valid(st_read(glue("/RSTOR/bfitzgerald/plume_shpfiles/unzipped/hms_smoke{yr}{mnth}{day}.shp"), quiet=T))
      cbgs_w_smoke_on_day <- st_join(us_block_groups, daily_smoke_plumes, st_intersects, left=F) %>% 
        st_set_geometry(NULL) %>%
        select(AFFGEOID, Density) %>%
        mutate(density=factor(Density, levels=c("Light", "Medium", "Heavy"), ordered=T)) %>%
        rename(census_block_group=AFFGEOID) %>% 
        group_by(census_block_group) %>%
        summarize(density=max(density)) %>% 
        mutate(smoke_month=str_c(yr, mnth), smoke_day=str_c(smoke_month, day))
      all_smoke_plume_cbgs <- bind_rows(all_smoke_plume_cbgs, cbgs_w_smoke_on_day)
      
    }, error=function(e){
      print(str_c("error on day:", as.character(each_day)))
      print(e)
    })
  }
}

#Trying again with the three that have malformed sf files: 
final_missing <- c(as.Date("2021-07-08"), as.Date("2022-10-05"), as.Date("2022-10-13"))
sf_use_s2(FALSE)
for(i in 1:length(final_missing)){
  each_day <- final_missing[[i]]
  yr    <- as.character(year(each_day))
  mnth  <- str_pad(month(each_day), width=2, pad="0", side="left")
  day   <- str_pad(mday(each_day), width=2, pad="0", side="left")
  if(each_day !=as.Date("2019-07-10")){
    tryCatch({
      daily_smoke_plumes <- st_make_valid(st_read(glue("/RSTOR/bfitzgerald/plume_shpfiles/unzipped/hms_smoke{yr}{mnth}{day}.shp"), quiet=T))
      cbgs_w_smoke_on_day <- st_join(us_block_groups, daily_smoke_plumes, st_intersects, left=F) %>% 
        st_set_geometry(NULL) %>%
        select(AFFGEOID, Density) %>%
        mutate(density=factor(Density, levels=c("Light", "Medium", "Heavy"), ordered=T)) %>%
        rename(census_block_group=AFFGEOID) %>% 
        group_by(census_block_group) %>%
        summarize(density=max(density)) %>% 
        mutate(smoke_month=str_c(yr, mnth), smoke_day=str_c(smoke_month, day))
      all_smoke_plume_cbgs <- bind_rows(all_smoke_plume_cbgs, cbgs_w_smoke_on_day)
      
    }, error=function(e){
      print(str_c("error on day:", as.character(each_day)))
      print(e)
    })
  }
}
sf_use_s2(TRUE)

## Write daily smoke plume cbgs to disk

write_parquet(all_smoke_plume_cbgs,"/RSTOR/bfitzgerald/plume_shpfiles/processed_daily_smoke_plume_cbgs.parquet")


## Aggregate daily smoke plume cbgs to monthly
monthly_cbgs_w_smoke_on_day <- all_smoke_plume_cbgs %>% 
  group_by(census_block_group, smoke_month) %>%
  summarize(
    n_days_with_smoke=n_distinct(smoke_day),
    n_days_with_smoke2=n(),
    n_days_with_heavy_smoke=sum(as.numeric(density=="Heavy"), na.rm=T),
    n_days_with_medium_smoke=sum(as.numeric(density=="Medium"), na.rm=T)) %>%
  mutate(
    n_days_with_light_smoke=n_days_with_smoke - n_days_with_heavy_smoke - n_days_with_medium_smoke
  )


monthly_cbgs_w_smoke_on_day <- monthly_cbgs_w_smoke_on_day %>% select(-c(n_days_in_month.x, n_days_in_month.y))
days_in_months <- data.frame(date=all_dates) %>%
  mutate(smoke_month=str_c(year(date), str_pad(month(date), width=2, pad="0", side="left"))) %>% 
  group_by(smoke_month) %>%
  summarize(n_days_in_month=n())

monthly_cbgs_w_smoke_on_day <- monthly_cbgs_w_smoke_on_day %>%
  left_join(days_in_months, by="smoke_month") %>% 
  mutate(pct_month_with_smoke       = n_days_with_smoke/n_days_in_month,
         pct_month_with_light_smoke = n_days_with_light_smoke/n_days_in_month,
         pct_month_with_heavy_smoke = n_days_with_heavy_smoke/n_days_in_month)

## Write monthly data to disk
write.csv(monthly_cbgs_w_smoke_on_day %>% mutate(month_start_on=str_c(str_sub(smoke_month, end=4), str_sub(smoke_month, start=5), "01",sep="-")),
          file="/RSTOR/bfitzgerald/plume_shpfiles/processed_monthly_cbgs_w_smoke.csv")
