################################################################################
# This is a piece of example code of how to download smoke plume from a URL and# 
# process it                                                                   #
################################################################################
library(tibble)
library(dplyr)
library(arrow)
library(sf)
library(ggplot2)
library(glue)
library(tigris)
library(stringr)

all_dates <- seq(as.Date("2019-01-01"), as.Date("2022-12-31"), by="days")
SMOKE_PLUME_DIR <- "/RSTOR/bfitzgerald/plume_shpfiles"

# removing Guam, virgin islands, and a few other US colonies.
us_colony_codes <- c(60, 72, 78, 66, 69) 
us_block_groups <- block_groups(cb=T) %>% 
  filter(!STATEFP %in% us_colony_codes) %>%
  st_transform(st_crs("EPSG:4326"))

pb = txtProgressBar(min = 0, max = length(all_dates), initial = 0) 

all_smoke_plume_cbgs <- data.frame()
error_days <- c()
error_messages <- c()

for(i in 1:length(all_dates)){
  each_day <- all_dates[[i]]
  yr    <- as.character(year(each_day))
  mnth  <- str_pad(month(each_day), width=2, pad="0", side="left")
  day   <- str_pad(mday(each_day), width=2, pad="0", side="left")
  tryCatch({
    if(!file.exists(glue("{SMOKE_PLUME_DIR}/unzipped/hms_smoke{yr}{mnth}{day}.shp"))){
      zipped_fp <- glue("{SMOKE_PLUME_DIR}/zipped/hms_smoke{yr}{mnth}{day}.zip")
      download.file(glue("https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/{yr}/{mnth}/hms_smoke{yr}{mnth}{day}.zip"), destfile=zipped_fp, quiet=T)
      unzip(glue("{SMOKE_PLUME_DIR}/zipped/hms_smoke{yr}{mnth}{day}.zip"), exdir="{SMOKE_PLUME_DIR}/unzipped/")
      file.remove(zipped_fp)
    }
    
    ## Next part does all the processing of the data and adds it to the overall dataset all_smoke_plume_cbgs
    daily_smoke_plumes <- st_make_valid(st_read(glue("{SMOKE_PLUME_DIR}/unzipped/hms_smoke{yr}{mnth}{day}.shp"), quiet=T))
    cbgs_w_smoke_on_day <- st_join(us_block_groups, daily_smoke_plumes, st_intersects, left=F) %>% 
      st_set_geometry(NULL) %>%
      select(AFFGEOID, Density) %>%
      mutate(density=factor(Density, levels=c("Light", "Medium", "Heavy"), ordered=T)) %>%
      rename(census_block_group=AFFGEOID) %>% 
      group_by(census_block_group) %>%
      summarize(density=max(density)) %>% 
      mutate(smoke_month=str_c(yr, mnth), smoke_day=str_c(smoke_month, day))
    
    
    # I add the results from the run to the other dataset
    all_smoke_plume_cbgs <- bind_rows(all_smoke_plume_cbgs, cbgs_w_smoke_on_day)
  }, error=function(e){
    # This adds the days where shit went wrong and what went wrong but without stopping everything. 
    error_days <- c(error_days, all_dates[i])
    error_messages <- c(error_messages, conditionMessage(e))
    NULL
  })
  
  setTxtProgressBar(pb,i)
}