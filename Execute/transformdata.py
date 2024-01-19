from flask import session, redirect, url_for, escape, request, render_template,flash,jsonify,make_response

from werkzeug.utils import secure_filename
import wheel
import pandas
import os
from Execute import queries,responses,middleware
import calendar
import time
import mysql.connector
import psycopg2

import json 
import simplejson as json #this is for desimal handling
from soarkEntry import spark,url,driver
from pyspark.sql import functions as F
# from pyspark.sql.functions import col, concat_ws
from pyspark.sql import Window
import cx_Oracle



class Json_formate:
        data_json={}
        data_keys={}

def transform(date):
        try:
                transformFC(date)
                transformCGD(date)
        except Exception as e:
                print("Error in Transforming data=================================",e) 

def transformFC(date):
        try:
                query = "(SELECT d_report_datetime, s_station_id, station_no, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_cvol', station_nm) AS float) AS s_cvol_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_period_cvol', station_nm) AS float) AS s_cvol_backup,CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup',d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_energy_gcv', station_nm) AS float) AS s_eng_gcv_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_period_energy_gcv', station_nm) AS float) AS s_eng_gcv_backup,CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_energy_ncv', station_nm) AS float) AS s_eng_ncv_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no,'backup', d_report_datetime, 's_period_energy_ncv', station_nm) AS float) AS s_eng_ncv_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_gcv_2', station_nm) AS float) AS s_avg_gcv_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_gcv_2', station_nm) AS float) AS s_avg_gcv_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_ncv_2', station_nm) AS float) AS s_avg_ncv_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_ncv_2', station_nm) AS float) AS s_avg_ncv_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_gas_velocity', station_nm) AS float) AS s_gas_velocity_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_gas_velocity', station_nm) AS float) AS s_gas_velocity_backup, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_duration', station_nm) AS float) AS s_duration_primary, CAST(dbo.FUN_GET_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_duration', station_nm) AS float) AS s_duration_backup, s_pit, s_tit,'FC' as s_station_type,s_site_name as s_station_name FROM dbo.VW_FLOW_METER_DATA where convert(varchar,cast([d_report_datetime] as date),105) = '{}') as data".format(date)
                # as data"
                # query1="(select * from tbl_fc_data) as data1"

                df = spark.read.format("jdbc").option("url", url).option("dbtable", query).option("driver", driver).load()

                station_ids=df.select("s_station_id").distinct().toPandas()['s_station_id']
                st_ids=list(station_ids)
                qres=queries.deleteFc(st_ids,date)
                print("FC Delete "+qres)
                print(st_ids)

                # df2 = spark.read.format("jdbc").option("url", url).option("dbtable", query1).option("driver", driver).load()

                # df_merge = df.unionAll(df2)
                # df_merge.show()
                
                df_merge = df.withColumn("_row_number", F.row_number().over(Window.partitionBy (df['s_station_id'],df['d_report_datetime']).orderBy('d_report_datetime')))
                # df_merge.show()

                df_final = df_merge.where(df_merge._row_number == 1).drop("_row_number")
                df_final=df_final.orderBy('d_report_datetime')
                # df_merge.orderBy('d_report_datetime').show()

                df_final.write.format("jdbc").options(url=url,driver=driver,dbtable="tbl_fcdata_temp").mode("append").save()
                print("Transformation of Fc data completed!!!!")
                # results=df.toJSON().map(lambda j: json.loads(j))
                # data=results.collect()
                # print(data)
        except Exception as e:
                print("Error in Transforming Flow Computer data=================================",e) 
                
def transformCGD(date):
        try:
                query = "(select *,'CGD' as s_station_type from(SELECT d_report_datetime, s_station_id, station_no,CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_cvol', station_nm)AS float) AS s_cvol_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_period_cvol', station_nm) AS float) AS s_cvol_backup,CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'backup',d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_backup, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_period_energy_gcv', station_nm) AS float)AS s_eng_gcv_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_period_energy_gcv', station_nm) AS float) AS s_eng_gcv_backup, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'backup', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_backup,CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no, 'Primary', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station_no,'backup', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_backup,s_site_name as s_station_name FROM [PRD_PIL_RPA].[dbo].[VW_STREAM_CGD_DATA] union all  SELECT  d_report_datetime, s_station_id, station, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'Primary', d_report_datetime, 's_period_cvol', station_nm) AS float) AS s_cvol_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'backup', d_report_datetime, 's_period_cvol', station_nm) AS float) AS s_cvol_backup,CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'Primary', d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'backup',d_report_datetime, 's_period_uvol', station_nm) AS float) AS s_uvol_backup, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'Primary', d_report_datetime, 's_period_energy_gcv', station_nm) AS float) AS s_eng_gcv_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'backup', d_report_datetime, 's_period_energy_gcv', station_nm) AS float) AS s_eng_gcv_backup, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'Primary', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'backup', d_report_datetime, 's_avg_meter_temp_tf', station_nm) AS float) AS s_meter_temp_backup, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station, 'Primary', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_primary, CAST(dbo.FUN_GET_CGD_PRIMARY_BACKUP_DATA(station,'backup', d_report_datetime, 's_avg_meter_press_pf', station_nm) AS float) AS s_meter_press_backup,s_site_name as s_station_name FROM  dbo.VW_STREAM_CGD_DATA WHERE station='FC3' ) as a where convert(varchar,cast(a.d_report_datetime as date),105) = '{}') as data".format(date)
                # as data"
                # query1="(select * from tbl_fc_data) as data1"

                df = spark.read.format("jdbc").option("url", url).option("dbtable", query).option("driver", driver).load()

                station_ids=df.select("s_station_id").distinct().toPandas()['s_station_id']
                st_ids=list(station_ids)
                qres=queries.deleteFc(st_ids,date)
                print("CGD Delete "+qres)
                # print(st_ids)
                # df2 = spark.read.format("jdbc").option("url", url).option("dbtable", query1).option("driver", driver).load()

                # df_merge = df.unionAll(df2)
                # df_merge.show()

                df_merge = df.withColumn("_row_number", F.row_number().over(Window.partitionBy (df['s_station_id'],df['station_no'],df['d_report_datetime']).orderBy('d_report_datetime')))
                # df_merge.show()

                df_final = df_merge.where(df_merge._row_number == 1).drop("_row_number")
                df_final=df_final.orderBy('d_report_datetime')
                # df_merge.orderBy('d_report_datetime').show()

                df_final.write.format("jdbc").options(url=url,driver=driver,dbtable="tbl_fcdata_temp").mode("append").save()
                print("Transformation of CGD data completed!!!!")
                # results=df.toJSON().map(lambda j: json.loads(j))
                # data=results.collect()
                # print(data)
        except Exception as e:
                print("Error in Transforming CGD data=================================",e) 
                