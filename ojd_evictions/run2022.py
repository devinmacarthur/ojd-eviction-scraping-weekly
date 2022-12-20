import os
import psycopg2
from psycopg2 import sql
from datetime import date, timedelta
import subprocess

today = date.today()
#os.getcwd()
#os.makedirs("G:\\My Drive\\DAILY_SCRAPE_OJD\\Daily_Cases_" + today.strftime("%m_%d_%Y"))
#os.makedirs("C:\\ojdevictions\\" + today + "\\2021_Court_Documents_" + today)

#make sql database
con = psycopg2.connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port= '5432')
con.autocommit = True
cur = con.cursor()
name_Database =  "ojdevictions_2022_" + today.strftime("%Y%m%d") # + "_2"
cur.execute("CREATE database "  + name_Database + ";")
con.close()

# call the crawl
# os.system("scrapy crawl ojd_evictions")

cmd = "scrapy"
arg1 = "crawl"
arg2 = "ojd-evictions-2022"
subprocess.call([cmd, arg1, arg2], shell = True)

# run r script to get tables and write to csv
command = "C:\\Program Files\\R\\R-4.0.5\\bin\\Rscript.exe"
path2script = "R_dataCleaning/OLD_flat_file_evictions.R"
subprocess.call([command, path2script], shell=True)

# get file extenstions
# cmd = os.getcwd() + "\\trid_w32\\trid.exe"
# path2 = "C:\\ojdevictions\\" + today + "\\2021_Court_Documents_" + today + "\\*"
# arg1 = "-ae"
#
# subprocess.call([cmd, path2, arg1], shell=True)
