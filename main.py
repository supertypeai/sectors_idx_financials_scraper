from urllib.request import urlopen
import sys
import os
import pandas as pd
import time
from multiprocessing import Process
from scraper_trading_view_financials import make_url, iterate_scrape, combine_data
from supabase import create_client
from dotenv import load_dotenv


DATA_DIR = os.path.join(os.getcwd(), "data")
load_dotenv()

if __name__ == "__main__":
  # Connection to Supabase
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Get the table
  db_data_MS = supabase.table("idx_financials_annual").select("symbol").eq("source", 3).execute()
  df_db_data = pd.DataFrame(db_data_MS.data)
  symbol_list : list = df_db_data['symbol'].unique().tolist()
  print(f"[DATABASE] Get {len(symbol_list)} data from database")

  df_need_search = pd.read_csv(os.path.join(DATA_DIR, "need_search.csv"))
  temp_symbol_list = df_need_search['symbol'].unique().tolist()

  for symbol in temp_symbol_list:
    if (symbol not in symbol_list):
      symbol_list.append(symbol)

  print(f"[DATA] There are {len(symbol_list)} data to be scrapped")

  try:
    # Read running argument
    if (sys.argv[1] == None):
      period_idx = 0
    else:
      period_idx = int(sys.argv[1])
      # Valid args [0 = annual, 1 = quarter]

    if (period_idx == 0 or period_idx == 1):

      length_list = len(symbol_list)
      i1 = int(length_list / 4)
      i2 = 2 * i1
      i3 = 3 * i1

      start = time.time()

      p1 = Process(target=iterate_scrape, args=(symbol_list[:i1], 1, period_idx))
      p2 = Process(target=iterate_scrape, args=(symbol_list[i1:i2], 2, period_idx))
      p3 = Process(target=iterate_scrape, args=(symbol_list[i2:i3], 3, period_idx))
      p4 = Process(target=iterate_scrape, args=(symbol_list[i3:], 4, period_idx))

      p1.start()
      p2.start()
      p3.start()
      p4.start()

      p1.join()
      p2.join()
      p3.join()
      p4.join()

      df = combine_data(period_idx)

      end = time.time()
      duration = int(end-start)
      print(f"[FINISHED] The execution time: {time.strftime('%H:%M:%S', time.gmtime(duration))}")
    else:
      print(f"[ERROR] False argument detected")
  except Exception as e:
    print(f"[ERROR] Failed to run the program: {e}")

  



    