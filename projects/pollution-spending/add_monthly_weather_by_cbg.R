######### Get weather information
library(ncdf4)
library(arrow)
library(tidyverse)
library(tigris)
library(sf)
library(stringr)
library(lubridate)
library(furrr)
require(data.table, include.only = "CJ")


process_raw_nc_files_from_gridmet <- function(input_directory = "/RSTOR/shared_data/gridmet/data/", 
                                              output_directory = "/RSTOR/bfitzgerald/daily_weather_by_cbg/"){
  
  years <- c(2019, 2020, 2021, 2022)
  measures <- c("pr", "rmax", "rmin", "tmin", "tmmn", "tmmx")
  
  #All gridmet files are on the same grid of lat and lons so grabbing one
  file.names <- (CJ(years, measures) %>%
    mutate(file_names=str_c('/RSTOR/shared_data/gridmet/data/', measures, "/", measures, "_", years,".nc"))
    )$file_names
  
  
  #Open the connection to the netCDF file
  nc <- nc_open(file.names[1])
  
  
  #Extract lat and lon vectors
  nc_lat <- ncvar_get(nc = nc, varid = "lat")
  nc_lon <- ncvar_get(nc = nc, varid = "lon")
  
  #Use the lat and lon vectors to create a grid represented as two vectors (note:
  #lon must go first to match with netcdf data)
  nc.coords <- expand.grid(lon=nc_lon,lat=nc_lat) %>% 
    mutate(lon = round(lon,5),
           lat = round(lat,5),
           cells=row_number())
  
  
  ##############################################
  #Use GIS tools to aggregate data by chosen geography
  ##############################################
  #Choose a projection to be used by all geographic files
  readin.proj=4326 #because it works with the lat and lons provided
  
  #Converting nc coordinates from vector form to simple feature (sf)
  g.nc.coords <- st_as_sf(nc.coords, coords = c("lon","lat")) %>% 
    st_set_crs(readin.proj)
  
  #ensure that county polygons are also in 4326
  us_colonies <- c("GU", "VI", "MP", "PR", "AS")
  us_colony_codes <- c(60, 72, 78, 66, 69)
  us_block_groups <- block_groups(cb=T) %>% 
    filter(!STATEFP %in% us_colony_codes) %>%
    st_transform(readin.proj)%>% 
    rename(cbg=GEOID)
  
  intersected <- st_join(g.nc.coords, us_block_groups, left=T) %>%
    select(cells, cbg) %>%
    filter(!is.na(cbg)) %>% 
    st_set_geometry(NULL)
  
  missing_block_groups <- us_block_groups %>% 
    filter(!(cbg %in% intersected$cbg))
  nearest_feature <- st_nearest_feature(missing_block_groups, g.nc.coords)

  closest_obs_for_missing_cbgs <- missing_block_groups %>% 
    mutate(cells=st_drop_geometry(g.nc.coords)[nearest_feature,]) %>%
    select(cells, cbg) %>% 
    st_set_geometry(NULL)
  
  
  full_block_lat_long_bridge <- rbind(intersected, closest_obs_for_missing_cbgs)
  
  fy = file.names[1]
  
  #Begin loop over variables (folders)
  plan(multisession(workers = 15))
  future_walk(
    file.names,
    function(fy){
      message(str_c("Beginning ",fy,"..."))
      
      vname=str_split(str_split(fy,"/")[[1]][5], "_")[[1]][1]
      
      #Construct the dataframe with all days and cells
      nc <- nc_open(fy)
      var.id=names(nc$var)
      date.vector <- as_date(nc$dim$day$vals,origin="1900-01-01")
      
      nc.data <- ncvar_get(nc = nc, varid = var.id)[,,]
      
      nc.data.df <- array(nc.data,dim=c(prod(dim(nc.data)[1:2]),dim(nc.data)[3])) %>%
        as_tibble(.name_repair = "universal") %>%
        rename_all(~str_c(date.vector))
      
      nc_lat <- ncvar_get(nc = nc, varid = "lat")
      nc_lon <- ncvar_get(nc = nc, varid = "lon")
      nc.coords <- expand.grid(lon=nc_lon,lat=nc_lat) %>% 
        mutate(lon = round(lon,5),
               lat = round(lat,5),
               cells=row_number())
      
      nc.df <- bind_cols(nc.coords,nc.data.df)
      
      #Join gridmet data with bridge 
      var.block <- inner_join(full_block_lat_long_bridge %>% select(cbg,cells),
                              nc.df %>% select(-c(lat,lon)),
                              by=c("cells")) %>%
        drop_na(cbg) %>%
        select(-c(cells)) %>%
        as.data.table() %>% 
        melt(., id.vars = c("cbg"),
             variable.name = "date",
             value.name = "value",
             variable.factor=FALSE) 
      
      out <- var.block[,.(value=base::mean(value,na.rm=T)),by=.(cbg,date)][,`:=`(var=vname)]
      
      dir_name = str_c(output_directory, vname)
      if(!dir.exists(dir_name)) dir.create(dir_name)
      write_parquet(out,paste0(dir_name,"/",vname,"_",str_sub(fy,-7,-4),".parquet"))
      
    },.progress = T)
}

build_monthly_flexible_weather_controls_dataset_by_cbg <-function(
    input_directory="/RSTOR/bfitzgerald/daily_weather_by_cbg/",
    output_file="/RSTOR/bfitzgerald/flexible_monthly_weather_by_cbg.parquet") {
  precip       <- open_dataset(str_c(input_directory, "pr/"))
  max_temp     <- open_dataset(str_c(input_directory, "tmmx/"))
  mean_temp    <- open_dataset(str_c(input_directory, "tmmn/"))
  max_humidity <- open_dataset(str_c(input_directory, "rmax/"))
  min_humidity <- open_dataset(str_c(input_directory, "rmin/"))
  
  monthly_precip <- precip %>%
    mutate(month = str_sub(date, end=7)) %>%
    group_by(month, cbg) %>%
    summarize(
      avg_daily_precip      = mean(value),
      avg_sq_daily_precip   = mean(value^2),
      
      n_days_0_mm_precip       = sum(if_else((value == 0),                 1, 0)),
      n_days_0_1_mm_precip     = sum(if_else((value > 0)  & (value <= 1),  1, 0)),
      n_days_1_2_mm_precip     = sum(if_else((value > 1)  & (value <= 2),  1, 0)),
      n_days_2_4_mm_precip     = sum(if_else((value > 2)  & (value <= 4),  1, 0)),
      n_days_4_8_mm_precip     = sum(if_else((value > 4)  & (value <= 8),  1, 0)),
      n_days_8_16_mm_precip    = sum(if_else((value > 8)  & (value <= 16), 1, 0)),
      n_days_16_30_mm_precip   = sum(if_else((value > 16) & (value <= 30), 1, 0)),
      n_days_30_plus_mm_precip = sum(if_else((value > 30),                 1, 0)),
    )
  
  monthly_max_temp <- max_temp %>%
    mutate(
      month = str_sub(date, end=7),
      value=value-273.15 # conversion from Kelvin to Celcius
    ) %>%
    group_by(cbg, month) %>%
    summarize(
      avg_daily_max_temp     = mean(value),
      avg_sq_daily_max_temp  = mean(value^2),
      
      n_days_0_less_max_deg_c  = sum(if_else((value <= 0),                 1, 0)),
      n_days_0_10_max_deg_c    = sum(if_else((value > 0)  & (value <= 10), 1, 0)),
      n_days_10_20_max_deg_c   = sum(if_else((value > 10) & (value <= 20), 1, 0)),
      n_days_20_25_max_deg_c   = sum(if_else((value > 20) & (value <= 25), 1, 0)),
      n_days_25_30_max_deg_c   = sum(if_else((value > 25) & (value <= 30), 1, 0)),
      n_days_30_35_max_deg_c   = sum(if_else((value > 30) & (value <= 35), 1, 0)),
      n_days_35_plus_max_deg_c = sum(if_else((value > 35),                 1, 0)),
    )
  
  monthly_mean_temp <- mean_temp %>%
    mutate(
      month = str_sub(date, end=7),
      value=value-273.15 # conversion from Kelvin to Celcius
    ) %>%
    group_by(cbg, month) %>%
    summarize(
      avg_daily_mean_temp     = mean(value),
      avg_sq_daily_mean_temp  = mean(value^2),
      
      n_days_0_less_mean_deg_c  = sum(if_else((value <= 0),                 1, 0)),
      n_days_0_10_mean_deg_c    = sum(if_else((value > 0)  & (value <= 10), 1, 0)),
      n_days_10_20_mean_deg_c   = sum(if_else((value > 10) & (value <= 20), 1, 0)),
      n_days_20_25_mean_deg_c   = sum(if_else((value > 20) & (value <= 25), 1, 0)),
      n_days_25_30_mean_deg_c   = sum(if_else((value > 25) & (value <= 30), 1, 0)),
      n_days_30_35_mean_deg_c   = sum(if_else((value > 30) & (value <= 35), 1, 0)),
      n_days_35_plus_mean_deg_c = sum(if_else((value > 35),                 1, 0)),
    )
  
  monthly_min_humidity <- min_humidity %>%
    mutate(month = str_sub(date, end=7)) %>%
    group_by(cbg, month) %>%
    summarize(
      avg_daily_min_humidity     = mean(value),
      avg_sq_daily_min_humidity  = mean(value^2),
      
      n_days_0_20_min_pct_rh    = sum(if_else((value <= 20),                1, 0)),
      n_days_20_40_min_pct_rh   = sum(if_else((value > 20) & (value <= 40), 1, 0)),
      n_days_40_60_min_pct_rh   = sum(if_else((value > 40) & (value <= 60), 1, 0)),
      n_days_60_80_min_pct_rh   = sum(if_else((value > 60) & (value <= 80), 1, 0)),
      n_days_80_100_min_pct_rh  = sum(if_else((value > 80),                 1, 0)),
    )
  
  monthly_max_humidity <- max_humidity %>%
    mutate(month = str_sub(date, end=7)) %>%
    group_by(cbg, month) %>%
    summarize(
      avg_daily_max_humidity     = mean(value),
      avg_sq_daily_max_humidity  = mean(value^2),
      
      n_days_0_50_max_pct_rh    = sum(if_else((value <= 50),                1, 0)),
      n_days_50_75_max_pct_rh   = sum(if_else((value > 50) & (value <= 75), 1, 0)),
      n_days_75_90_max_pct_rh   = sum(if_else((value > 75) & (value <= 90), 1, 0)),
      n_days_90_99_max_pct_rh   = sum(if_else((value > 90) & (value < 100), 1, 0)),
      n_days_100_max_pct_rh     = sum(if_else((value == 100),               1, 0)),
    )
  
  monthly_cbg_weather <- monthly_precip %>%
    full_join(monthly_max_temp,     by=c("cbg", "month")) %>%
    full_join(monthly_mean_temp,    by=c("cbg", "month")) %>%
    full_join(monthly_max_humidity, by=c("cbg", "month")) %>%
    full_join(monthly_min_humidity, by=c("cbg", "month"))
  
  write_parquet(monthly_cbg_weather, output_file)
}

process_raw_nc_files_from_gridmet(output_directory="/RSTOR/bfitzgerald/daily_weather_by_cbg/")
monthly_weather <- build_monthly_flexible_weather_controls_dataset_by_cbg(
  input_directory="/RSTOR/bfitzgerald/daily_weather_by_cbg/",
  output_file="/RSTOR/bfitzgerald/flexible_monthly_weather_by_cbg.parquet")



