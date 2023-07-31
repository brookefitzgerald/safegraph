import numpy as np
import pandas as pd
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

import json

from safegraph_py_functions import safegraph_py_functions as sgpy


def generate_online_and_transaction_intermediary_dfm():
  
  with open("/home/bfitzgerald/safegraph/scripts/constants.json", "r") as read_content:
      constants = json.load(read_content)
  
  
  # Overall python documentation: https://arrow.apache.org/docs/python/getstarted.html
  # Useful information about arrow compute functions: https://arrow.apache.org/docs/python/api/compute.html#api-compute
  
  
  ### No pure arrow method to go from string to map in arrow
  #ty = pa.map_(pa.string(), pa.float64())
  #spend_full["spend_by_transaction_intermediary"].cast(ty) # fails with error pyarrow.lib.ArrowNotImplementedError: parsing scalars of type map<string, string>
  
  
  spend_full  = pq.read_table(constants["SPEND_DIR"] + '/', columns=["placekey","spend_date_range_start", "raw_total_spend", "online_spend", "spend_by_transaction_intermediary"])
  spend_full = spend_full.set_column(spend_full.num_columns, "spent_on_date", 
    pc.utf8_slice_codeunits(
      pc.cast(spend_full["spend_date_range_start"], pa.string()),
      start=0,
      stop=10))
  columns=['placekey', 'statefp', 'countyfp', 'has_complete_panel', "sub_category", "is_continental_usa", "n_months_with_spend"]#, "naics_code"]
  
    
    
  table1 = pq.ParquetDataset(constants["PLACES_DIM_DIR"] + "/naics_code=452311").read(columns=columns)
  table1 = table1.set_column(7, "naics_code", pa.array(np.repeat(452311, table1.num_rows)))
  table2 = pq.ParquetDataset(constants["PLACES_DIM_DIR"] + "/naics_code=722513").read(columns=columns)
  table2 = table2.set_column(7, "naics_code", pa.array(np.repeat(722513, table2.num_rows)))
  table3 = pq.ParquetDataset(constants["PLACES_DIM_DIR"] + "/naics_code=722511").read(columns=columns)  
  table3 = table3.set_column(7, "naics_code", pa.array(np.repeat(722511, table3.num_rows)))
  table4 = pq.ParquetDataset(constants["PLACES_DIM_DIR"] + "/naics_code=445110").read(columns=columns)  
  table4 = table4.set_column(7, "naics_code", pa.array(np.repeat(445110, table4.num_rows)))
  
  places_grocery = pa.concat_tables([table1, table2, table3, table4])
  places_grocery = places_grocery.filter(
    pc.and_(
      places_grocery["is_continental_usa"], 
      pc.greater(places_grocery["n_months_with_spend"], 0)
    ))
  places_grocery = places_grocery.set_column(8, "full_county_id", pc.binary_join_element_wise(
      pc.cast(places_grocery["statefp"], pa.string()),
      pc.cast(places_grocery["countyfp"], pa.string()),
      ""
  ))
  
  
  spend_grocery = spend_full.join(places_grocery, keys="placekey")
  del spend_full
  spend_grocery = spend_grocery.set_column(spend_grocery.num_columns, "spend_by_transaction_intermediary_real", pc.replace_substring(spend_grocery["spend_by_transaction_intermediary"], '""', '"'))
  
  
  # To use the safegraph helper functions we have to convert the dataframe to pandas
  spend_grocery = spend_grocery.to_pandas()
  
  unpacked_intermediaries = sgpy.unpack_json_fast(spend_grocery, "spend_by_transaction_intermediary_real")
  
  
  # Since the data is now in pandas, we can use the pandas syntax to create the intermediary columns
  int_none = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="No intermediary"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_none"]      = int_none
  spend_grocery["int_instacart"] = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Instacart"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_doordash"]  = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="DoorDash"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_grubhub"]   = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Grubhub"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_postmates"] = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Postmates"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_orderup"]   = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="OrderUp"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_seamless"]  = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Seamless"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_yelp"]      = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Yelp"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_olo"]       = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Olo"].spend_by_transaction_intermediary_real_value
  spend_grocery["int_favor"]     = unpacked_intermediaries[unpacked_intermediaries["spend_by_transaction_intermediary_real_key"]=="Favor"].spend_by_transaction_intermediary_real_value
  
  groups = spend_grocery.groupby(["full_county_id", "naics_code", "spent_on_date"])
  results = pd.concat([
    groups[["online_spend", "raw_total_spend", "int_none", "int_instacart", "int_doordash", "int_grubhub", "int_postmates", "int_orderup", "int_seamless", "int_yelp", "int_olo", "int_favor"]].sum(),
    groups[["placekey"]].count()
  ])
  del spend_grocery
  del places_grocery
  return(results)

def time_aggregation():
  start = time.time()
  generate_online_and_transaction_intermediary_dfm()
  end = time.time()

  print(end - start)

  # 388.68495535850525 seconds or 6.5 minutes, but with significant delay before 
  # the server is usable again (likely the background processes of trasforming variables into R/python)
  #But when I immediately run this: 
  newend = time.time()
  print(newend - start)
  # 510.3427755832672 or 8.5 minutes
