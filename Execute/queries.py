from Execute import executeSql,responses,middleware
import platform

#getting all user data
def getAllUserData():
     try:
          sql="SELECT * from tbl_user_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllUserData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all function data
def getAllFunctions():
     try:
          sql="SELECT * from tbl_function_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllFunctions query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all customer for filter
def getCustomerFilter():
     try:
          sql="SELECT * FROM tbl_customer_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getCustomerFilter query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
#for login
def login(data):
     try:
          sql="exec proc_login @s_user_id =? ,@s_password =?"
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in Login query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#________________________________For Process Header________________________________________
#getting all process header data
def getAllProcessHeader(ptype):
     try:
          if ptype != '':
               sql="SELECT * from tbl_process_header where s_os_type=%s and s_process_type=%s"
               data=(platform.system(),ptype)
          else:
               sql="SELECT * from tbl_process_header where s_os_type=%s"
               data={platform.system()}

          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllProcessHeader query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#adding process header record
def addProcessHeader(data):
     try:
          sql="INSERT INTO tbl_process_header(s_file_name,s_file_path,s_file_type,s_index_name,s_delimeter,s_os_type,s_target_datasource,s_target_data_table,s_process_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteReturnId(sql,data)
          return msgs
     except Exception as e:
          print("Error in addProcessHeader query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#adding process detail record
def addProcessDetail(data):
     try:
          sql = "INSERT INTO tbl_process_child(n_process_head_id,s_column_name,s_column_type,s_table_column_name) values(%s,%s,%s,%s)"
          msgs=executeSql.ExecuteMany(sql, data)
          return msgs
     except Exception as e:
          print("Error in addProcessDeail query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')
    
#getting process data by id
def getProcessById(id):
     try:
          data={id}
          sql="SELECT * FROM tbl_process_header where n_process_head_id=%s"
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getProcessById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#deleting process 
def delProcess(id):
     try:
          sql="delete a,b from tbl_process_header a left join tbl_process_child b on  b.n_process_head_id=a.n_process_head_id where a.n_process_head_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in delprocess query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#getting process child data
def getprocessdata(id):
     try:
          sql="select * from tbl_process_child where n_process_head_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getProcessdata query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#get datasource 
def getDataSource():
     try:
          sql="select * from tbl_datasource"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getdataSource query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting database type
def getdbType(id):
     try:
          data={id}
          sql="select * from tbl_datasource where n_datasource_id=%s"
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getdbType query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#________________________________For Process Header close________________________________________


#________________________________For Process Scheduler___________________________________________

#getting all process scheduler record
def getAllProcessScheduler():
     try:
          os=platform.system()
          sql="select a.*,b.s_file_name FROM tbl_process_scheduler a,tbl_process_header b where a.n_process_head_id=b.n_process_head_id and s_os_type=%s"
          data={os}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllProcessScheduler query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     
#for Adding Scheduler Record
def addProcessScheduler(data):
     try:
          #sql="INSERT INTO tbl_process_scheduler(n_process_head_id,d_start_date,s_schedule_type,d_schedule_date,t_schedule_time,t_schedule_day) values(%s,%s,%s,%s,%s,%s)"
          sql="call upsertprocess(%s,%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in addProcessScheduler query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#for deleting Scheduler Record
def delProcessScheduler(id):
     try:
          sql="delete from tbl_process_scheduler where n_process_schedule_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in delProcessScheduler query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#for geting Scheduler Record by id
def getProScheById(id):
     try:
          sql="select * from tbl_process_scheduler where n_process_schedule_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getproschebyid query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#for updating process scheduler
def updateProcessScheduler(data):
     try:
          sql="UPDATE tbl_process_scheduler SET n_process_head_id=%s, t_schedule_time=%s, t_schedule_day=%s, d_start_date=%s WHERE n_process_schedule_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in updateProcessScheduler query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

#________________________________For Process Scheduler close______________________________________

#________________________________For Process Authorization______________________________________
#getting All Authorization data
def getAuthorizationData():
     try:
          sql="SELECT a.*,b.s_file_name FROM tbl_authorization a,tbl_process_header b where a.n_process_head_id=b.n_process_head_id"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAuthorizationData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting region data by id
def regionById(id):
     try:
          sql="SELECT region_fun(%s)"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in regionbyId query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#Adding authorization data
def addAuthorization(data):
     try:
          sql="INSERT INTO tbl_authorization(n_process_head_id,s_user_id,s_filter) values(%s,%s,%s)"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in addAuthorization query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#getting Auth data by id
def getAuthDataById(id):
     try:
          sql= "select * from tbl_authorization where n_authorization_id=%s"
          data=(id)
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAuthDataById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#updating Auth data
def updateAuthData(data):
     try:
          sql="UPDATE tbl_authorization SET n_process_head_id=%s, s_user_id=%s, s_filter=%s WHERE n_authorization_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in UpdateAuthData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

#deleting Auth data
def delAuthData(id):
     try:
          sql="delete from tbl_authorization where n_authorization_id=%s"
          data=(id)
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in delAuthData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#getting global data
def getGlobalData():
     try:
          sql= "select * from tbl_global_region"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getGlobalData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


#getting filter of other table data
def getAllFilters():
     try:
          sql= "select * from tbl_global_region where s_other_table is not null and s_other_table !=''"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getGlobalData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting filter of other table data
def getAllDashFilters(id):
     try:
          sql= "select * from tbl_global_region where s_other_table is not null and s_other_table !='' and find_in_set(s_field,(select s_filters from tbl_dashboard_header_master where n_dashboard_id=%s))"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getGlobalData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getFilterValues(data):
     try:
          sql= "select "+data[0]+" from "+data[1]
          data=''
          msgs=executeSql.ExecuteAll(sql,data)
          return msgs
     except Exception as e:
          print("Error in getGlobalData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



#getting region filter data
def getRegionFilter():
     try:
          sql= "select * from tbl_region_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getRegionFilter query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
#________________________________For Process Authorization close______________________________________

#________________________________For Component Master_________________________________________________
#Get all Component data
def getAllComponentData():
     try:
          sql="select * from tbl_component_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllComponentData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#save component master data
def saveCompRecord(data):
     try:
          sql="INSERT INTO tbl_component_master(s_component_name,s_script,s_component_icon,s_calling_component,s_property_data) values(%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteReturnId(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#save Component child data
def addCompChildData(data):
     try:
          sql = "INSERT INTO tbl_component_child_master(n_component_id,s_perameter,s_input_type) values(%s,%s,%s)"
          if type(data) == tuple:
               msgs=executeSql.ExecuteOne(sql,data)
          else :
               msgs=executeSql.ExecuteMany(sql,data)
          
          return msgs
     except Exception as e:
          print("Error in addCompChildData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#deleting component record
def DeleteCompRec(id):
     try:
          sql="delete a,b from tbl_component_master a left join tbl_component_child_master b on  b.n_component_id=a.n_component_id where a.n_component_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in DeleteCompRec query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#get component data by id
def getComponentDataByID(id):
     try:
          sql="select a.*,GROUP_CONCAT(b.s_perameter,';',b.n_component_child_id,';',b.s_input_type) as perameters from tbl_component_master a left join tbl_component_child_master b on a.n_component_id=b.n_component_id where a.n_component_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getComponentDataByID query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

#delete comp perameter
def delCompPerameter(id):
     try:
          sql="delete from tbl_component_child_master where n_component_child_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in delCompPerameter query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#update comp record
def updateCompRecord(data):
     try:
          sql="UPDATE tbl_component_master SET s_component_name=%s, s_script=%s,s_component_icon=%s,s_calling_component=%s,s_property_data=%s WHERE n_component_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in updateCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def getPropertyByOption(data):
     try:
          sql="SELECT * FROM tbl_component_property where find_in_set(%s,s_applicable_on)"
          msgs=executeSql.ExecuteAllNew(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in getPropertyByOption query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getAllComponentProperty():
     try:
          sql="SELECT * FROM tbl_component_property"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in getAllComponentProperty query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
#________________________________For Component Master close___________________________________________

#________________________________For Widget Master ___________________________________________________

#getting Widget data
def getAllWidgetData(type):
     try:
          #sql="select * from tbl_widget"
          sql="SELECT a.*,b.s_widget_grp_name,getfilternames(a.s_filters) FROM tbl_widget a,tbl_widget_group b where a.n_widget_grp_id=b.n_widget_grp_id "
          if type =='Es':
               sql+=" and a.n_datasource_id is null"
          elif type == '' :
               sql+=" and a.n_datasource_id is not null"
          sql+=" order by a.n_widget_grp_id"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllWidgetData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#save widget data
def SaveWidget(data):
     try:
          sql = "INSERT INTO tbl_widget(n_data_modal_id,s_widget_name,s_widget_desc,s_index_name_or_query,n_component_id,s_script,s_customized_query,s_manual_script,n_datasource_id,s_perameter,n_widget_grp_id,s_style_column,s_head_obj,s_calculated_field,s_filters,s_comp_property,s_date_filter,s_refresh_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in SaveWidget query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#getting widget data by id
def getWidgetById(id):
     try:
          # sql="select * from tbl_widget where n_widget_id=%s"
          sql="SELECT a.*, b.s_component_name,b.s_script as finalScript,getfilternames(a.s_filters) FROM tbl_widget a , tbl_component_master b where a.n_component_id = b.n_component_id and a.n_widget_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getWidgetById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def deleteWidgetData(id):
     try:
          # sql="select * from tbl_widget where n_widget_id=%s"
          sql="delete FROM tbl_widget where n_widget_id="+id
          data=''
          msgs=executeSql.ExecuteOne(sql,data)
          if msgs == responses.execution_200:
               sql2="update tbl_dashboard_widget set n_widget_id='' where n_widget_id="+id
               data2=''
               msgs2=executeSql.ExecuteOne(sql2,data2)
               results=middleware.query_msgs(msgs2)
          else:
               results=middleware.query_msgs(msgs)

          return results
     except Exception as e:
          print("Error in deleteWidgetData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
#________________________________For Widget Master close______________________________________________

#================================For Header Master====================================================
#getting dash header data 
def getDashHeaderData():
     try:
          sql="SELECT a.*,b.s_menu_name from tbl_dashboard_header_master a,tbl_menu_master b where a.n_menu_id=b.n_menu_id"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getDashHeaderData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#adding dash header data
def addDashHeaderMaster(data):
     try:
          sql="INSERT INTO tbl_dashboard_header_master(s_dashboard_name,s_url,s_icon_name,s_dimension,n_menu_id,n_seq,s_filters,s_position) values(%s,%s,%s,%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteReturnId(sql,data)
          return msgs
     except Exception as e:
          print("Error in addDashHeaderMaster query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

# getting dash header data by id
def getDashHeaderMasterById(id):
     try:
          sql="select * from tbl_dashboard_header_master where n_dashboard_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getDashHeaderMasterById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

# updating headers data
def updateHeaderMasterData(data):
     try:
          sql="UPDATE tbl_dashboard_header_master SET s_dashboard_name=%s,s_url=%s,s_icon_name=%s, s_dimension=%s,n_menu_id=%s,n_seq=%s,s_filters=%s,s_position=%s WHERE n_dashboard_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in updateHeaderMasterData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

# deleting header data
def deleteHeaderMasterData(id):
     try:
          sql = "delete from tbl_dashboard_header_master where n_dashboard_id=%s"
          data = {id}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in deleteHeaderMasterData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
#save dashboard page with widget and div id
def SavePageRecord(data):
     try:
          sql = "INSERT INTO tbl_dashboard_widget(n_dashboard_id,s_div_script,n_widget_id) values(%s,%s,%s) ON DUPLICATE KEY UPDATE n_widget_id=values(n_widget_id)"
          if type(data) == tuple:
               msgs=executeSql.ExecuteOne(sql,data)
          else :
               msgs=executeSql.ExecuteMany(sql,data)
          return msgs
     except Exception as e:
          print("Error in SavePageRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

# updating page record
def UpdatePageRecord(data):
     try:
          sql = "Update tbl_dashboard_widget set n_widget_id=%s where n_dashboard_id=%s and s_div_script=%s"
          if type(data) == tuple:
               msgs=executeSql.ExecuteOne(sql,data)
          else :
               msgs=executeSql.ExecuteMany(sql,data)

          results=middleware.query_msgs(msgs)
          return results 
     except Exception as e:
          print("Error in UpdatePageRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

def getWidgetfiltername(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql='select * FROM tbl_global_region where n_global_seq in(%s) and s_other_table is not null and s_compare_type = "="'
          data={data}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getwidgetfilter query ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


# get dash widget
def getDashWidget(id):
     try:
          sql="select * from tbl_dashboard_widget where n_dashboard_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getDashWidget query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
# getting global filter by id
def getGlobalFilterByField(data):
     try:
          sql='select * from tbl_global_region where s_field in ("' + '","'.join(map(str, data)) + '")'
          #sql='select *,case when s_other_table != "" then getFilterData(s_other_table) else s_value end as value from tbl_global_region where s_field in ("' + '","'.join(map(str, data)) + '")'
          d=''
          msgs=executeSql.ExecuteAll(sql,d)
          return msgs
     except Exception as e:
          print("Error in getGlobalFilterByField query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# getting Regions data
def getRegionsData(data):
     try:
          if len(data) == 0:
               sql="select * from tbl_region_master"
               d=''
          elif len(data) > 0:
          # sql="SELECT * FROM db_nexta_bi.tbl_region_master where n_region_id in (SELECT CAST(e AS CHAR(10000) CHARACTER SET utf8) FROM split_string_into_rows WHERE split_string_into_rows((select s_filter from tbl_authorization where s_user_id=%s and n_process_head_id=%s)))"
               sql="select * from tbl_region_master where n_region_id in (" + ",".join(data) + ")"
               d=''
          msgs=executeSql.ExecuteAllNew(sql,d)
          return msgs
     except Exception as e:
          print("Error in getRegionsData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting customer data
def getCustomer(data):
     try:
          if len(data) == 0:
               sql="select * from tbl_customer_master"
               d=''
          elif len(data) > 0:
          # sql="SELECT * FROM db_nexta_bi.tbl_region_master where n_region_id in (SELECT CAST(e AS CHAR(10000) CHARACTER SET utf8) FROM split_string_into_rows WHERE split_string_into_rows((select s_filter from tbl_authorization where s_user_id=%s and n_process_head_id=%s)))"
               sql="select * from tbl_customer_master where n_customer_id in (" + ",".join(data) + ")"
               d=''
          msgs=executeSql.ExecuteAll(sql,d)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting global filter by user id
def getGlobalFilterByFieldByUserID(id):
     try:
          sql='SELECT * FROM tbl_authorization where s_user_id = %s'
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting dash auth data by user
def getDashAuthDataByUser(data):
     try:
          sql="select * from tbl_dashboard_authorization where s_user_id=%s and n_dashboard_id=%s"
          msgs=executeSql.ExecuteAll(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#================================End Header Master====================================================

#================================For Dashboard Authorization====================================================

# getting dash auth data
def getDashboardAuthData():
     try:
          sql="SELECT a.*, b.s_dashboard_name FROM tbl_dashboard_header_master b,tbl_dashboard_authorization a where a.n_dashboard_id=b.n_dashboard_id"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#grtting dash auth 
def addDashAuthorizationData(data):
     try:
          sql="INSERT INTO tbl_dashboard_authorization(n_dashboard_id,s_user_id,s_filter) values(%s,%s,%s) ON DUPLICATE KEY UPDATE s_filter=VALUES(s_filter)"
          msgs=executeSql.ExecuteMany(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#update dash auth data
def updateDashAuthData(data):
     try:
          sql="UPDATE tbl_dashboard_authorization SET s_filter=%s WHERE n_dashboard_id=%s and s_user_id=%s and n_dash_auth_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
#getting dash auth data by id
def getDashAuthDataById(id):
     try:
          sql="select * from tbl_dashboard_authorization where n_dash_auth_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

# delete dash auth data
def deleteDashAuthData(id):
     try:
          sql="delete from tbl_dashboard_authorization where n_dash_auth_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in saveCompRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
#================================End Dashboard Authorization====================================================


#================================For Dashboard Scheduler====================================================

# get dash scheduler header
def getDashboardSchedulerHeader():
     try:
          sql="SELECT a.*, s_dashboard_name FROM tbl_dashboard_header_master b,tbl_dashboard_scheduler a where a.n_dashboard_id=b.n_dashboard_id"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getDashboardSchedulerHeader query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# add dash scheduler data
def addDashSchedulerData(data):
     try:
          #sql="INSERT INTO tbl_dashboard_scheduler(n_dashboard_id,t_dash_schedule_time,t_dash_schedule_day,d_dash_start_date) values(%s,%s,%s,%s)"
          sql="call upsertprocess1(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
          msgs=executeSql.ExecuteReturnId(sql,data)
          return msgs
     except Exception as e:
          print("Error in addDashSchedulerData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')


#get data schedule by id
def getdashSchById(id):
     try:
          sql="select * from tbl_dashboard_scheduler where n_dashboard_scheduler_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getdashSchById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


# update dashboard scheduler
def updatedashSch(data):
     try:
          sql="UPDATE tbl_dashboard_scheduler SET n_dashboard_id=%s, t_dash_schedule_time=%s, t_dash_schedule_day=%s, d_dash_start_date=%s WHERE n_dashboard_scheduler_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in updatedashSch query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
#deleted dash scheduler
def DeletedashSch(id):
     try:
          sql="delete from tbl_dashboard_scheduler where n_dashboard_scheduler_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
               print("Error in DeletedashSch query==========================",e)
               return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
#get filter with dash id

def getfiltername(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql="SELECT * FROM tbl_dashboard_header_master where n_dashboard_id =%s "
          data={data}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#get user with dashboard id 

def getbyfilters(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql="SELECT a.*, b.s_first_name ,b.s_email_id FROM tbl_dashboard_authorization a,tbl_user_master b where a.s_user_id = b.s_user_id and a.n_dashboard_id=%s "
          data={data}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getScheudleValue(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql="SELECT * FROM tbl_dashboard_scheduler where n_dashboard_id=%s and s_dash_type=%s and s_dash_schedule_type=%s and s_filter_name=%s "
          data=(data)
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getting Shceduler data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
#================================Ends Dashboard Scheduler====================================================



#________________________________For Menu master______________________________________
#getting All menu master data
def getMenuMasterData():
     try:
          sql="SELECT * from tbl_menu_master"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:
          print("Error in getMenuMasterData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
 

#getting All menu master data
def addMenuMasterData(data):
     try:
          sql="INSERT INTO tbl_menu_master(s_menu_name,s_icon_name) values(%s,%s)"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:
          print("Error in addMenuMasterData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')
 
#get Icon data
def getIconData():
     try:
          sql="SELECT * from tbl_icon_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
               print("Error in getIconData query==========================",e)
               return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
 
#deleting menu data
def deleteMenuData(id):
     try:
          sql="delete from tbl_menu_master where n_menu_id=%s"
          data={id}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in deleteMenuData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

#updating menu master data
def updateMenuMasterData(data):
     try:
          sql="UPDATE tbl_menu_master SET s_menu_name=%s, s_icon_name=%s WHERE n_menu_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in updateMenuMasterData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
 
#getting menu master by id
def getMenuMasterById(id):
     try:
          sql="select * from tbl_menu_master where n_menu_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getMenuMasterById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
 


#===============================================Widget Group Queries ==================================

# add widget group data
def addWidgetGroupData(data):
     try:
          sql="INSERT INTO tbl_widget_group(s_widget_grp_name) values(%s)"
          data={data}
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in addWidgetGroupData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')
 
# get widget group data
def getWidgetGroupData():
     try:
          sql="SELECT * from tbl_widget_group"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getWidgetGroupData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
 

# update widget group record
def updatewidgetgroupRecord(data):
     try:
          sql="UPDATE tbl_widget_group SET s_widget_grp_name=%s WHERE n_widget_grp_id=%s"
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in updatewidgetgroupRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
 
# get widget group by id
def getWidgetGroupById(id):
     try:
          sql="select * from tbl_widget_group where n_widget_grp_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getWidgetGroupById query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
 

# delete widget group data
def deleteWidgetGroupData(id):
     try:
          sql = "delete from tbl_widget_group where n_widget_grp_id=%s"
          data={id}
          msgs = executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in deleteWidgetGroupData query=============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')


#===============================================Widget Group Queries ==================================

# get regions
def getRegions(data):
     try:
          data=data.split(",")
          sql='select * from tbl_region_master where n_region_id in ("' + '","'.join(data) + '")'
          d=''
          msgs=executeSql.ExecuteAll(sql,d)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in getRegions query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# get customers
def getCustomers(data):
     try:
          data=data.split(",")
          sql='select * from tbl_customer_master where n_customer_id in ("' + '","'.join(data) + '")'
          d=''
          msgs=executeSql.ExecuteAll(sql,d)
          results=middleware.query_msgs(msgs)
          return results
     except Exception as e:
          print("Error in getCustomers query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

 #________________________________For Data Source Header________________________________________

#adding process header record

def saveDSDetail(data):
     try:
          sql="INSERT INTO tbl_datasource(s_database_type,s_database_name,s_user_name,s_password,s_hostname,s_port,s_service_name) values(%s,%s,%s,%s,%s,%s,%s)"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

#get data of datasource

def getDSData():
     try:

          sql="SELECT * from tbl_datasource"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getDSById(id):
     try:

          sql="select * from tbl_datasource where n_datasource_id=%s"
          data={id}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def deleteDSData(id):
     try:
          sql="delete from tbl_datasource where n_datasource_id=%s"
          data={id}
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')



def updateDSData(data):
     try:
          sql="UPDATE tbl_datasource SET s_database_type=%s,s_database_name=%s,s_user_name=%s,s_password=%s,s_hostname=%s, s_port=%s,s_service_name=%s WHERE n_datasource_id=%s"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

# ------------------------------------------Filter Master-------------------------------------------
def getstatedata():
     try:

          sql="exec PROC_GET_STATE"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getLocdata():
     try:

          sql="exec PROC_GET_LOCATION"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getuserdata():
     try:

          sql="exec PROC_GET_USER"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getstationdata():
     try:

          sql="exec PROC_GET_STATION"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
def getcustomerdata():
     try:

          sql="exec PROC_GET_CUSTOMER_DATA"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getDSDATA():
     try:

          sql="exec PROC_GET_DS"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getdashcountdata():
     try:

          sql="exec PROC_GET_DASHBOARD_COUNT"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getDSlistdata():
     try:

          sql="exec PROC_GET_STATION_SCHEDULAR_DATA"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getlocbyid(id):
     try:

          sql="EXECUTE PROC_GET_LOCATION_BY_ID @n_location_id=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def getdownloadexceldata(id):
     try:

          sql="EXECUTE PROC_GET_SCH_DATA_BY_ID @sid=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def getuserbyid(id):
     try:

          sql="EXECUTE PROC_GET_USER_BY_ID @n_user_id=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def getstationbyid(id):
     try:

          sql="EXECUTE PROC_GET_STATION_BY_ID @n_station_id=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getcustomerdatabyid(id):
     try:

          sql="EXECUTE PROC_GET_CUSTOMER_DATA_BY_ID @n_customer_email_id=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getStationName():
     try:

          sql="EXECUTE PROC_GET_STATION_NAME"
          #data1={id}
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in getting station name ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def getstatebyid(id):
     try:

          sql="EXECUTE PROC_GET_STATE_BY_ID @n_state_id=?"
          #data1={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def deletestatebyid(id):
     try:
          sql="exec PROC_DELETE_STATE_BY_ID @n_state_id=?"
          results=executeSql.ExecuteOne(sql,id)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

def deletelocbyid(id):
     try:
          sql="exec PROC_DELETE_LOCATION_BY_ID @n_location_id=?"
          results=executeSql.ExecuteOne(sql,id)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

def deleteuserbyid(id):
     try:
          sql="exec PROC_DELETE_USER_BY_ID @n_user_id=?"
          results=executeSql.ExecuteOne(sql,id)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

def deletestationbyid(id):
     try:
          sql="exec PROC_DELETE_STATION_BY_ID @n_station_id=?"
          results=executeSql.ExecuteOne(sql,id)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
def deletecustomerdatabyid(id):
     try:
          sql="exec PROC_DELETE_CUS_DATA_BY_ID @n_customer_email_id=?"
          results=executeSql.ExecuteOne(sql,id)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')


def updatestatedata(data):
     try:
          sql="exec PROC_UPDATE_STATE_MASTER @n_state_id=?,@s_state_name=?,@s_updated_by=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def updateuserdata(data):
     try:
          sql="exec PROC_UPDATE_USER_MASTER @s_user_id=?, @s_emp_no=?,@s_first_name=? ,@s_last_name=?,@s_email_id=?,@s_role=?,@s_contact_no=?,@s_password=?,@s_status=?,@s_updated_by=?,@n_user_id=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def updatestationdata(data):
     try:
          sql="exec PROC_UPDATE_STATION_MASTER @s_location_id=?,@s_station_no=?,@s_station_name=? ,@s_station_ip=?,@s_station_type=?,@s_file_type=?,@s_file_name=?,@s_station_user_id=?,@s_station_password=?,@s_status=?,@s_updated_by=?,@n_station_id=?,@s_sub_station_type=?,@s_meter_name_number=?,@s_stream_station1=?,@s_stream1_name=?,@s_stream_station2=?,@s_stream2_name=?,@s_stream_1_type1=?,@s_stream_2_type2=?,@s_pgts_name=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def updatecustomerdatabyid(data):
     try:
          sql="exec PROC_UPDATE_CUS_DATA_MASTER @s_station_id=?,@s_m_r_name=?,@s_to_email=? ,@s_cc_email=?,@n_customer_email_id=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')


def updateLocdata(data):
     try:
          sql="exec PROC_UPDATE_LOCATION_MASTER @s_state_id=?,@s_location_name=?,@s_updated_by=?,@n_location_id=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def insertpgtsdata(data):
     try:
          sql="PROC_INST_MEASUREMENT_ENTRY_RPA"
          print(data)
          results=executeSql.ExecuteOrcOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def getpgtsmtrvaldata(data):
     try:
          sql="PROC_GET_METER_DATA_RPA"
          results=executeSql.ExecuteOrcgetall(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')


def getallstation():
     try:
          sql="PROC_GET_STATION_PGTS"
          results=executeSql.ExecuteAllNew1(sql)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def savestatedata(data):
     try:
          sql="exec PROC_INSERT_STATE @s_state_name=? , @s_created_by=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def savelocdata(data):
     try:
          sql="exec PROC_INSERT_LOCATION @s_state_id=? ,@s_location_name=?, @s_created_by=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def saveuserdata(data):
     try:
          sql="exec PROC_INSERT_USER @s_user_id=?,@s_emp_no=?,@s_first_name=? ,@s_last_name=?,@s_email_id=?,@s_role=?,@s_contact_no=?,@s_password=?,@s_status=?,@s_created_by=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def getinvmatchdata():
     try:

          sql="exec PROC_GET_INV_MATCHING"
          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def saveinvoicematching(data):
     try:
          sql="exec PROC_INSERT_INV_MATCHING @FROM_DATE=?,@TO_DATE=?,@CREATED_BY=?,@ROLE=?,@STATUS=?,@UNMATCH_STG1=?,@MATCH_STG1=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')



def savestationdata(data):
     try:
          sql="exec PROC_INSERT_STATION @s_location_id=?,@s_station_no=?,@s_station_name=? ,@s_station_ip=?,@s_station_type=?,@s_file_type=?,@s_file_name=?,@s_station_user_id=?,@s_station_password=?,@s_status=?,@s_created_by=?,@s_process_type=?,@s_sub_station_type=?,@s_meter_name_number=?,@s_stream_station1=?,@s_stream1_name=?,@s_stream_station2=?,@s_stream2_name=?,@s_stream_1_type1=?,@s_stream_2_type2=?,@s_pgts_name=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')
def savecustomerdata(data):
     try:
          sql="exec PROC_INSERT_CUS_DATA @s_station_id=?,@s_m_r_name=?,@s_to_email=? ,@s_cc_email=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')
              
def saveChecklistRecord(data):
     try:
          sql="exec PROC_INSERT_STATION_SCHEDULE @s_station_array=?,@d_schedule_datetime=?,@s_created_by=?"
          print(data)
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def savescheduleRecord(data):
     try:
          sql="exec PROC_INSERT_SCHEDULE @s_gtc=?,@s_gas_analysis=?,@s_gtc_rh=?,@n_status=?,@s_rerun_status=?,@d_schedule_datetime=?,@s_created_by=?,@n_gtc_status=?,@n_gas_analysis_status=?,@n_gtc_rh_status=?,@n_msi_rpt_1=?,@n_msi_rpt_2=?,@n_mnr_rpt=?,@n_lpk_sug=?,@n_or_rpt_1=?,@n_or_rpt_2=?,@n_w_f=?,@n_week_trip=?,@n_f_inv=?,@n_measurement_val=?,@n_ciii_rpt=?,@n_con_file_1=?,@n_con_file_2=?"
          print(data)
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def saveChecklistRecord_test(data):
     try:
          sql="exec PROC_INSERT_STATION_SCHEDULE_TEST @s_station_array=?,@d_schedule_datetime=?,@s_created_by=?,@fc=?,@usm=?,@gc_update=?,@gc_calib=?"
          print(data)
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')
              
def getChecklistRecord():
     try:
          sql="EXECUTE PROC_GET_SCH_DATA"


          msgs=executeSql.ExecuteAllNew1(sql)
          return msgs
     except Exception as e:

            print("Error in getting Scheduler data ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')
              


def gettable():
     try:
          #sql="SELECT * FROM information_schema.schemata"
          sql="SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = 'db_nexta_bi'"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def gettablefields(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql="SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME =%s "
          data={data}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def checkfilterStatus(data):
     try:
          #sql="SELECT * FROM information_schema.schemata"
          
          sql="SELECT s_field FROM tbl_global_region where s_field =%s "
          data={data}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
# ------------------------------------------Filter Master-------------------------------------------

# ------------------------------------------User Master---------------------------------------------


def getUserData():
     try:

          sql="SELECT * from tbl_user_master"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getpgtsname(id):
     try:

          sql="EXEC PROC_GET_PGTS_STATION_NAME @s_staion_id=? "
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getStationIds(id):
     try:

          # sql="PROC_GET_FLOW_CONTROL_STATIONIDS @s_station_name=?"
          sql="select station_id,case when station_no='FC3' then 3 else rowno end as rowno,station_no from [dbo].[FUN_GET_FLOW_CONTROL_STATIONIDS](?) union All select s_station_id as station_id,case when s_sub_station_type='CGD' then 3 else '' end as rowno,'FC3' as s_station_no from tbl_station_master where s_station_name =? and s_station_no='FC1' and s_station_name !='AGI GREENPAC'"

          # data={id}
          msgs=executeSql.ExecuteAllNew(sql,id)
          return msgs
     except Exception as e:

          print("Error in getting station ids data in queries file ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def upsertFc(data):
     try:
          sql="exec [PROC_UPSERT_FC] @s_station_id=? , @d_report_datetime=?,@s_station_no=?,@s_cvol_primary=?,@s_eng_gcv_primary=?,@s_eng_ncv_primary=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')

def upsertFcCalc(data):
     try:
          sql="exec [PROC_UPSERT_FC_CALCULATION] @s_station_id=? , @d_report_datetime=?,@s_station_no=?,@s_cvol_primary=?,@s_eng_gcv_primary=?,@s_eng_ncv_primary=?"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

            print("Error in insert datasource ==============================",e)
            return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021500')


def getuserById(id):
     try:

          sql="select * from tbl_user_master where n_user_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def deleteuserData(id):
     try:
          sql="delete from tbl_user_master where n_user_id=%s"
          data={id}
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')



def updateUserMasterData(data):
     try:
          sql="UPDATE tbl_user_master SET s_user_id=%s,s_first_name=%s,s_last_name=%s,s_email_id=%s,s_password=md5(%s) WHERE n_user_id=%s"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def addUserData(data):
     try:
          sql="INSERT INTO tbl_user_master(s_user_id,s_first_name,s_last_name,s_email_id,s_password) values(%s,%s,%s,%s,md5(%s))"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')

# ------------------------------------------User Master---------------------------------------------

# ------------------------------------------Data Modal----------------------------------------------

def addDatamodal(data):
     try:
          sql="INSERT INTO tbl_data_modal(n_data_source_id,s_data_modal_name,s_query,s_created_by,d_created_date) values(%s,%s,%s,%s,%s)"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')



def getdatamodal():
     try:

          sql="SELECT * from tbl_data_modal"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def getdatamodalById(id):
     try:

          sql="select * from tbl_data_modal where n_data_modal_id=%s"
          data={id}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def updateDataModal(data):
     try:
          sql="UPDATE tbl_data_modal SET n_data_source_id=%s,s_data_modal_name=%s,s_query=%s,s_modified_by=%s,d_modified_date=%s WHERE n_data_modal_id=%s"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def deleteDataModal(id):
     try:
          sql="delete from tbl_data_modal where n_data_modal_id=%s"
          data={id}
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')


#---------------------------------------------data modal-------------------------------------------


# ------------------------------------------Data Modal Authorization----------------------------------------------

def addDatamodalauth(data):
     try:
          sql="INSERT INTO tbl_data_modal_authorization(n_data_modal_id,n_user_id,s_created_by,d_created_date) values(%s,%s,%s,%s)"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')




def getdatamodalauth():
     try:

          #sql="SELECT * from tbl_data_modal_authorization"
          sql="SELECT a.*,b.s_user_id,c.s_data_modal_name from tbl_data_modal_authorization a,tbl_user_master b,tbl_data_modal c where a.n_user_id=b.n_user_id and a.n_data_modal_id=c.n_data_modal_id"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def getdatamodalauthById(id):
     try:

          sql="select * from tbl_data_modal_authorization where n_data_modal_auth_id=%s"
          data={id}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def updateDataModalauth(data):
     try:
          sql="UPDATE tbl_data_modal_authorization SET n_data_modal_id=%s,n_user_id=%s,s_modified_by=%s,d_modified_date=%s WHERE n_data_modal_auth_id=%s"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')


def deleteDataModalauth(id):
     try:
          sql="delete from tbl_data_modal_authorization where n_data_modal_auth_id=%s"
          data={id}
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')


def getDataModalByUser(uid):
     try:
          sql="SELECT a.n_data_modal_auth_id,c.* from tbl_data_modal_authorization a,tbl_data_modal c where a.n_data_modal_id=c.n_data_modal_id and a.n_user_id=%s"
          data={uid}
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')




#---------------------------------------------data modal Authorization-------------------------------------------

def getReportingData(id):
     try:

          sqlFC="EXECUTE PROC_GET_FC_REPORT_DATA @s_station_name=?"
          #data1={id}
          msgsFC=executeSql.ExecuteAllNew(sqlFC,id)

          sqlGC="EXECUTE PROC_GET_GC_REPORT_DATA @s_station_name=?"
          #data1={id}
          msgsGC=executeSql.ExecuteAllNew(sqlGC,id)

          sqlMAIN="EXECUTE [PROC_GET_MAINT_INCEPT_REPORT_DATA] @s_station_name=?"
          msgsMAIN=executeSql.ExecuteAllNew(sqlMAIN,id)

          sqlFS="EXECUTE [PROC_GET_FS_REPORT_DATA] @s_station_name=?"
          msgsFS=executeSql.ExecuteAllNew(sqlFS,id)
          
          msgs=[msgsFC,msgsGC,msgsMAIN,msgsFS]
          return msgs
     except Exception as e:

          print("Error in getting reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getStationIdbyName(id):
     try:

          sqlstation="select s_station_id from tbl_station_master where s_station_name=?"
          #data1={id}
          station=executeSql.ExecuteRaw(sqlstation,id)

          return station
     except Exception as e:

          print("Error in getting reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getStationIdbyNameType(id):
     try:

          sqlstation="select s_station_id from tbl_station_master where s_station_name=? and s_station_type=?"
          #data1={id}
          station=executeSql.ExecuteRaw(sqlstation,id)

          return station
     except Exception as e:

          print("Error in getting reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def deleteFc(id,date):
     try:
          sts_ids=",".join("'{0}'".format(x) for x in id)
          sql="delete from tbl_fc_data where s_station_id in( {} ) and convert(varchar,cast([d_report_datetime] as date),105)=?".format(sts_ids)
          
          # data={date}
          msgs=executeSql.ExecuteOne(sql,date)
          return msgs
     except Exception as e:
          print("Error in delprocess query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

def upsertTicketValidation(data):
     try:
          sql="exec PROC_UPSERT_JOIN_TICKET_VALIDATION @s_location_name=?,@d_report_datetime=?,@s_checked_by=?,@d_checked_date=?,@d_calibration_date=?,@d_next_calibration_date=?,@s_temp=?,@s_pressure=?,@s_updated_by=?"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in upsert ticket validation ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def saveGcData(data):
     try:
          sql="exec PROC_UPSERT_GC @s_station_id=?, @d_report_datetime=?,@s_type=? ,@s_avg=?,@s_min=?,@s_max=?,@s_sample=?,@s_site_nm=?,@user_id=?"
                
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in upsert ticket validation ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')


def getTicketValidation(data):
     try:
          sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          results1=executeSql.ExecuteAllNew(sql,data)
          sql2="select [dbo].[FUN_GET_LAST_CALIBRATION_DATE](?) as cal_date;"
          results2=executeSql.ExecuteAllNew(sql2,data[0])
          results=[results1,results2]
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def getpm_lanned_data(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)
          sql2="SELECT s_site_name as cs_name,[dbo].[get_sch_cnt_cs_wise_mtd](s_site_name,?) as sch_cnt_mtd,[dbo].[get_sch_cnt_cs_wise_ytd](s_site_name,?) as sch_cnt_ytd,[dbo].[get_unpr_cnt_cs_wise_mtd](s_site_name,?)as mtd_unpr_cnt,[dbo].[get_unpr_cnt_cs_wise_ytd](s_site_name,?) ytd_unpr_cnt,[dbo].[get_cmp_cnt_cs_wise_mtd](s_site_name,?) as mtd_cmp_cnt,[dbo].[get_cmp_cnt_cs_wise_ytd](s_site_name,?) as ytd_cmp_cnt,[dbo].[get_shcld_cnt_cs_wise_mtd](s_site_name,?) as mtd_sh_cld,[dbo].[get_shcld_cnt_cs_wise_ytd](s_site_name,?) as ytd_sh_cld,[dbo].[get_pend_7days_mtd](s_site_name,?) as mtd_pend_cnt FROM tbl_iw_three_nine_pm_data  group by s_site_name order by cs_name asc;"
          results2=executeSql.ExecuteAllNew(sql2,id)
          results=results2
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_preserve_data(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)
          sql2="SELECT s_site_name as cs_name,[dbo].[get_sch_cnt_cs_wise_mtd_wth_str](s_site_name,?) as sch_cnt_mtd_wth_str,[dbo].[get_sch_cnt_cs_wise_ytd_wth_str](s_site_name,?) as sch_cnt_ytd_wth_str,[dbo].[get_unpr_cnt_cs_wise_mtd_wth_str](s_site_name,?)as mtd_unpr_cnt,[dbo].[get_unpr_cnt_cs_wise_ytd_wth_str](s_site_name,?) ytd_unpr_cnt,[dbo].[get_cmp_cnt_cs_wise_mtd_wth_str](s_site_name,?) as mtd_cmp_cnt,[dbo].[get_cmp_cnt_cs_wise_ytd_wth_str](s_site_name,?) as ytd_cmp_cnt,[dbo].[get_shcld_cnt_cs_wise_mtd_wth_str](s_site_name,?) as mtd_sh_cld,[dbo].[get_shcld_cnt_cs_wise_ytd_wth_str](s_site_name,?) as ytd_sh_cld,[dbo].[get_pend_7days_mtd_wth_str](s_site_name,?) as mtd_pend_cnt  FROM tbl_iw_three_nine_pm_data  group by s_site_name order by cs_name asc"
          results2=executeSql.ExecuteAllNew(sql2,id)
          results=results2
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_noti_data(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)
          sql2="SELECT s_site_name as cs_name,[dbo].[get_sch_noti_cnt_cs_wise_mtd](s_site_name,?) as sch_cnt_mtd,[dbo].[get_sch_noti_cnt_cs_wise_ytd](s_site_name,?) as sch_cnt_ytd,[dbo].[get_unpr_noti_cnt_cs_wise_mtd](s_site_name,?)as mtd_unpr_cnt,[dbo].[get_unpr_noti_cnt_cs_wise_ytd](s_site_name,?) ytd_unpr_cnt,[dbo].[get_cmp_noti_cnt_cs_wise_mtd](s_site_name,?) as mtd_cmp_cnt,[dbo].[get_cmp_noti_cnt_cs_wise_ytd](s_site_name,?) as ytd_cmp_cnt,[dbo].[get_shcld_noti_cnt_cs_wise_mtd](s_site_name,?) as mtd_sh_cld,[dbo].[get_shcld_noti_cnt_cs_wise_ytd](s_site_name,?) as ytd_sh_cld,'' as ytd_comp_per,[dbo].[get_pend_noti_30days_mtd](s_site_name,?) as mtd_pend_cnt  FROM tbl_iw_two_nine_pm_data  group by s_site_name order by cs_name asc"
          results2=executeSql.ExecuteAllNew(sql2,id)
          results=results2
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_ageing_data(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)
          sql2="SELECT s_site_name as cs_name,sum(s_0_7_days) as zero_7_days_cnt,sum(s_8_15_days) as eight_15_days_cnt,sum(s_16_30_days) as sixteen_30_days_cnt,sum(s_31_60_days) as thirt1_60_days_cnt,sum(s_61_90_days) as sixty1_90_days_cnt,sum(s_91_180_days) as ninety1_180_days_cnt,sum(s_181_366_days) as one81_366_days_cnt,sum(s_366_days) as three66_days_cnt,sum(s_total_orders_days) as total_or_days,sum(s_no_of_times_re_sche) as s_no_of_times_re_sche,(sum(s_no_of_times_re_sche)/sum(s_total_orders_days)) as s_no_of_times_re_sche_avg FROM [PRD_PIL_RPA].[dbo].[tbl_zorder_ageing_pm_data] where convert(varchar,s_irst_start_date,23)  between concat(cast(case when datepart(mm,?) in (4,5,6,7,8,9,10,11,12) then DATEPART(yy,?) else DATEPART(yy,?)-1  end as varchar) ,'-04-01') and EOMONTH(?) and  s_irst_start_date<>s_basic_start_date group by s_site_name order by cs_name asc;"
          results2=executeSql.ExecuteAllNew(sql2,id)
          sql1="select STRING_AGG(a.site_name,',') as cs_name,STRING_AGG(a.cnt,',') as cnte from(select  s_site_name as site_name,count(*) as cnt from tbl_zorder_ageing_pm_data where s_maintactivitytype='PC4' group by s_site_name) as a "
          results1=executeSql.ExecuteAllNew1(sql1)
          sql3="select count(*) as cnt from tbl_zorder_ageing_pm_data where month(s_irst_start_date)=month(?)"
          results3=executeSql.ExecuteAllNew(sql3,id[0])
          results=[results1,results2,results3]
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_BOM_quantity(id):
     try:
          sql="select s_site_name as cs_name, [dbo].[get_bom_cnt](s_site_name,'INS','More than BOM quantity','PM01') as ins,[dbo].[get_bom_cnt](s_site_name,'IPA','More than BOM quantity','PM01') as ipa,[dbo].[get_bom_cnt](s_site_name,'MEC','More than BOM quantity','PM01') as MECH,(cast([dbo].[get_bom_cnt](s_site_name,'ELE','More than BOM quantity','PM01') as int)+cast([dbo].[get_bom_cnt](s_site_name,'PIMS','More than BOM quantity','PM01')as int)) as ele_pims,'' as grand_total  FROM tbl_yeam_bom_pm_data group by s_site_name order by cs_name asc;"
          results1=executeSql.ExecuteAllNew1(sql)
          sql2="select s_site_name as cs_name, [dbo].[get_bom_cnt](s_site_name,'INS','More than BOM quantity','PM02') as ins,[dbo].[get_bom_cnt](s_site_name,'IPA','More than BOM quantity','PM02') as ipa,[dbo].[get_bom_cnt](s_site_name,'MEC','More than BOM quantity','PM02') as MECH,(cast([dbo].[get_bom_cnt](s_site_name,'ELE','More than BOM quantity','PM02') as int)+cast([dbo].[get_bom_cnt](s_site_name,'PIMS','More than BOM quantity','PM02')as int)) as ele_pims,'' as grand_total  FROM tbl_yeam_bom_pm_data group by s_site_name order by cs_name asc;"
          results2=executeSql.ExecuteAllNew1(sql2)
          results=[results1,results2]
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_BOM_outside(id):
     try:
          sql="select s_site_name as cs_name, [dbo].[get_bom_cnt](s_site_name,'INS','Outside BOM','PM01') as ins,[dbo].[get_bom_cnt](s_site_name,'IPA','Outside BOM','PM01') as ipa,[dbo].[get_bom_cnt](s_site_name,'MEC','Outside BOM','PM01') as MECH,(cast([dbo].[get_bom_cnt](s_site_name,'ELE','Outside BOM','PM01') as int)+cast([dbo].[get_bom_cnt](s_site_name,'PIMS','Outside BOM','PM01')as int)) as ele_pims,'' as grand_total FROM tbl_yeam_bom_pm_data group by s_site_name order by cs_name asc;"
          results1=executeSql.ExecuteAllNew1(sql)
          sql2="select s_site_name as cs_name, [dbo].[get_bom_cnt](s_site_name,'INS','Outside BOM','PM02') as ins,[dbo].[get_bom_cnt](s_site_name,'IPA','Outside BOM','PM02') as ipa,[dbo].[get_bom_cnt](s_site_name,'MEC','Outside BOM','PM02') as MECH,(cast([dbo].[get_bom_cnt](s_site_name,'ELE','Outside BOM','PM02') as int)+cast([dbo].[get_bom_cnt](s_site_name,'PIMS','Outside BOM','PM02')as int)) as ele_pims,'' as grand_total FROM tbl_yeam_bom_pm_data group by s_site_name order by cs_name asc;"
          results2=executeSql.ExecuteAllNew1(sql2)
          results=[results1,results2]
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getpm_confirm(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)where month(s_created_on)=month(convert(varchar,cast('? as date),105)) and year(s_created_on)=year(?) 
          sql2=" SELECT s_site_name ,[dbo].[get_pm_conf_tot](s_site_name,?) as apm_grand_tot,[dbo].[get_pm_conf_othr](s_site_name,?) as bpm_others ,0 as cbpm_others,[dbo].[get_pm_cnfirm_done_jc](s_site_name,?) as job_cmp,[dbo].[get_pm_cmp_done_jc] (s_site_name,?) as pm_done,[dbo].[get_pm_men_data](s_site_name,?) as pm_ment FROM tbl_iw_four_seven_pm_data_new  group by s_site_name order by s_site_name asc;"
          results2=executeSql.ExecuteAllNew(sql2,id)
          results=results2
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def getorder_cnt(id):
     try:
          # sql="exec PROC_GET_JOIN_TICKET_VALIDATION @s_location_nm=?,@d_reporting_date=?;"
          # results1=executeSql.ExecuteAllNew(sql,data)where month(s_created_on)=month(convert(varchar,cast('? as date),105)) and year(s_created_on)=year(?) 
          sql="SELECT sum(cast(s_amount_in_lc as decimal)) as amt_totl,count(distinct s_order) as order_cnt FROM [PRD_PIL_RPA].[dbo].[tbl_mb_fifty_four] where s_movement_type='262' and (s_wbs_element like'%OPEX%' or s_wbs_element like'%CAPEX%');"
          results1=executeSql.ExecuteAllNew1(sql)
          results=results1
          sql2="SELECT sum(cast(s_amount_in_lc as decimal)) as amt_totl,count(distinct s_order) as order_cnt FROM [PRD_PIL_RPA].[dbo].[tbl_mb_fifty_four] where s_movement_type='262' and (s_wbs_element not like'%OPEX%' or s_wbs_element not  like'%CAPEX%');"
          results2=executeSql.ExecuteAllNew1(sql2)
          results=[results1,results2]
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')
def sendJticketMail(data):
     try:
          sql="exec PROC_GET_STATION_MAIL_DATA @station_id=?"
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in getting ticket validation data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def getGegData(id):
     try:

          sqlGEG="EXECUTE PROC_GET_GEG_REPORT_DATA @s_start_date=?,@s_end_date=?,@type=?,@cs=?,@equipment=?,@sample_point=?,@oil_type=?;"
          #data1={id}
          msgsGEG=executeSql.ExecuteAllNew(sqlGEG,id)

          msgs=[msgsGEG]
          return msgs
     except Exception as e:

          print("Error in getting reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getMachineData():
     try:

          sqlGEG="select s_machine_name,n_class, STUFF(( SELECT ',' + concat(T.s_status,'~',T.s_condition) FROM tbl_class_master T WHERE a.n_class = T.s_class FOR XML PATH('')), 1, 1, '') as classdetail from tbl_machine_master a order by s_machine_name"
          data1=[]
          msgsGEG=executeSql.ExecuteAllNew(sqlGEG,data1)

          msgs=[msgsGEG]
          return msgs
     except Exception as e:

          print("Error in getting reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def getViberation(id):
     try:

          sqlGEG="EXECUTE PROC_GET_VIBRATION_REPORT_DATA @s_start_date=?,@s_end_date=?,@s_machine=?,@s_location=?;"
          #data1={id}
          msgsGEG=executeSql.ExecuteAllNew(sqlGEG,id)

          msgs=[msgsGEG]
          return msgs
     except Exception as e:

          print("Error in getting vibration reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def getVibrationLocation(id):
     try:

          sqlGEG="EXECUTE PROC_GET_VIBRATION_LOCATION_DATA @s_start_date=?,@s_end_date=?,@s_machine=?,@s_location=?,@location=?;"
          #data1={id}
          msgsGEG=executeSql.ExecuteAllNew(sqlGEG,id)

          msgs=[msgsGEG]
          return msgs
     except Exception as e:

          print("Error in getting vibration reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


def insertGegData(id):
     try:

          sqlGEG="exec PROC_UPSERT_GEG_COMPARISION @company_name=?,@registration_no=?,@sample_no=?,@submission_date=?,@receipt_date=?,@report_date=?,@lubrication=?,@equipment_ref=?,@equipment_desc=?,@component_desc=?,@recommendation=?,@sample_date=?,@equipment_life_km_hr=?,@oil_life=?,@viscocity_40_c=?,@viscocity_100_c=?,@viscocity_indx=?,@tan_mg=?,@water_cntent=?,@size_5_15=?,@size_15_25=?,@size_25_50=?,@size_50_100=?,@size_gr_100=?,@classna1638=?,@flast_pnt=?,@oil_wtr_em_rt=?,@time_fr_separa_min=?,@test_temp=?,@color=?,@alppm=?,@bappm=?,@bppm=?,@cdppm=?,@cappm=?,@crppm=?,@cuppm=?,@feppm=?,@pbppm=?,@mgppm=?,@mnppm=?,@moppm=?,@nippm=?,@pppm=?,@sippm=?,@nappm=?,@snppm=?,@tippm=?,@vppm=?,@znppm=?,@appeareance=?,@fire_pnt=?,@chloride=?,@created_by=?"
          #data1={id}
          msgsGEG=executeSql.ExecuteOne(sqlGEG,id)

          msgs=[msgsGEG]
          return msgs
     except Exception as e:

          print("Error in getting vibration reporting data ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


#getting all getAllEquipment data
def getAllEquipment():
     try:
          sql="SELECT * from tbl_equipment where 1=?"
          data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllEquipment query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all getGraphdata data
def getGraphdata(data):
     try:
          # "SELECT a.s_oil_life,b.s_attribute_name  from tbl_oil_header a left join tbl_oil_child b on a.n_sample_id=b.n_sample_id where  a.s_cs_location=?' and a.s_equipment=? and a.s_sampling_point=? and a.s_sample_type=? and a.s_oil_grade=? and b.s_attribute_name=?"
          sql="SELECT b.s_value,b.s_attribute_name  from tbl_oil_header a left join tbl_oil_child b on a.n_sample_id=b.n_sample_id where  a.s_cs_location=? and a.s_equipment=? and a.s_sampling_point=? and a.s_sample_type=? and a.s_oil_grade=? and b.s_attribute_name=? and (format(cast(s_sample_date as date) ,'yyy-MM-dd') between format(cast(? as date) ,'yyy-MM-dd')  and format(cast(? as date) ,'yyy-MM-dd'))"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getGraphdata query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all Machine status vibration data
def getMachineStatus(data):
     try:
          sql="SELECT COUNT(1) AS CNT,S_MACHINE_CON FROM TBL_VIBRATION_HEADER WHERE S_SCHEDULE_DATE=? GROUP BY S_MACHINE_CON"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in Machine status vibration query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all cs wise machine status data
def getCSMachineStatus(data):
     try:
          sql="SELECT COUNT(1) AS CNT,S_MACHINE_CON,S_CS_LOCATION FROM TBL_VIBRATION_HEADER WHERE S_SCHEDULE_DATE=? GROUP BY S_MACHINE_CON,S_CS_LOCATION "
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in cs wise machine status query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
          
#getting all cs wise machine status data
def getCSMachStatus(data):
     try:
          sql="SELECT COUNT(1) AS CNT,S_MACHINE_CON,S_MACHINE_NAME,S_SORT_NM,S_CS_LOCATION FROM TBL_VIBRATION_HEADER WHERE S_SCHEDULE_DATE=? GROUP BY S_MACHINE_CON,S_MACHINE_NAME,S_SORT_NM,S_CS_LOCATION "
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in cs & machine wise machine status query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting all MAchine wise machine status data
def getMchMachineStatus(data):
     try:
          sql="SELECT COUNT(1) AS CNT,S_MACHINE_CON,S_MACHINE_NAME,S_SORT_NM FROM TBL_VIBRATION_HEADER WHERE S_SCHEDULE_DATE=?  GROUP BY S_MACHINE_CON,S_MACHINE_NAME,S_SORT_NM"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in MAchine wise machine status query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting over all Oil status data
def getOilStatus(data):
     try:
          if data[2]=='':
               newdata=(data[0],data[1])
               sql="select count(s_status) as CNT,(case when (((sum(s_status)/count(s_status))*100=100)) then 'normal' else 'alert' end) as s_status,s_equipment,s_sample_type from (SELECT (case when b.s_status='normal' then 1 else 0 end) as s_status,a.s_equipment,a.s_sample_type  from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=?) as c group by c.s_equipment,c.s_sample_type "
          else:
               newdata=(data)
               sql="select count(s_status) as CNT,(case when (((sum(s_status)/count(s_status))*100=100)) then 'normal' else 'alert' end) as s_status,s_equipment,s_sample_type from (SELECT (case when b.s_status='normal' then 1 else 0 end) as s_status,a.s_equipment,a.s_sample_type  from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id  where cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? and s_equipment=? ) as c group by c.s_equipment ,c.s_sample_type "
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in Oil status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


#getting cs wise Oil status data
def getCSOilStatus(data):
     try:
          if data[2]=='':
               newdata=(data[0],data[1])
               sql=" select sum(CNT) as CNT,s_status,s_cs_location from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,a.s_cs_location from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? group by a.s_cs_location,b.s_status) as c group by c.s_status,c.s_cs_location order by c.s_status desc"
          else:
               newdata=(data)
               sql=" select sum(CNT) as CNT,s_status,s_cs_location from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,a.s_cs_location from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? and s_equipment=?  group by a.s_cs_location,b.s_status) as c group by c.s_status,c.s_cs_location order by c.s_status desc"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in cs wise Oil status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


#getting  Oil type wise status data
def getOiltypeStatus(data):
     try:
          if data[2]=='':
               newdata=(data[0],data[1])
               sql="select sum(CNT) as CNT,s_status,s_sample_type from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,upper(a.s_sample_type) as s_sample_type from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and  cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? group by b.s_status,upper(a.s_sample_type)) as c group by c.s_status,c.s_sample_type order by c.s_status desc;"
          else:
               newdata=(data)
               sql="select sum(CNT) as CNT,s_status,s_sample_type from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,upper(a.s_sample_type) as s_sample_type from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and  cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? and s_equipment=?  group by b.s_status,upper(a.s_sample_type)) as c group by c.s_status,c.s_sample_type order by c.s_status desc;"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in  Oil type wise status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting  attr wise status data
def getOilattrStatus(data):
     try:
          if data[2]=='':
               newdata=(data[0],data[1])
               sql="select sum(CNT) as CNT,s_status,s_attribute_name from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,b.s_attribute_name from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=?  group by b.s_status,b.s_attribute_name) as c group by c.s_status,c.s_attribute_name order by c.s_status desc;"
          else:
               newdata=(data)
               sql="select sum(CNT) as CNT,s_status,s_attribute_name from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,b.s_attribute_name from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? and s_equipment=? group by b.s_status,b.s_attribute_name) as c group by c.s_status,c.s_attribute_name order by c.s_status desc;"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in  attr wise status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#getting  Oil date wise status data
def getdatewiseStatus(data):
     try:
          if data[3]=='':
               newdata=(data[0],data[1],data[2])
               sql="select sum(CNT) as CNT,s_status,s_sample_date from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,cast(a.s_sample_date as date) as s_sample_date from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where cast(a.s_sample_date as date) between  cast(DATEADD(month, -5, ?) as date) and cast(? as date) and b.s_status <> '' and s_cs_location=? group by cast(a.s_sample_date as date),b.s_status) as c group by c.s_status,c.s_sample_date order by c.s_status desc;"
          else:
               newdata=(data)
               sql="select sum(CNT) as CNT,s_status,s_sample_date from (SELECT COUNT(1) AS CNT,(case when b.s_status='normal' then 'normal' else 'alert' end) as s_status,cast(a.s_sample_date as date) as s_sample_date from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where cast(a.s_sample_date as date) between  cast(DATEADD(month, -5, ?) as date) and cast(? as date) and b.s_status <> '' and s_cs_location=? and s_equipment=? group by cast(a.s_sample_date as date),b.s_status) as c group by c.s_status,c.s_sample_date order by c.s_status desc;"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in  Oil date wise status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


#getting  Oil detail wise status data
def getOildetaildata(data):
     try:
          if data[2]=='':
               newdata=(data[0],data[1])
               sql="select * from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=?"
          else:
               newdata=(data)
               sql="select * from tbl_oil_header a inner join tbl_oil_child b on a.n_sample_id=b.n_sample_id where 1=1 and cast(a.s_sample_date as date)=? and b.s_status <> '' and s_cs_location=? and s_equipment=?"
          # data='1'
          msgs=executeSql.ExecuteAllNew(sql,newdata)
          return msgs
     except Exception as e:
          print("Error in  Oil detail wise status  query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

################  ticket ##############
def submit_issue(data):
     try:
          sql = "INSERT INTO tbl_issue_data (s_area, s_gtc_name, s_remark, d_due_date, s_status,s_modified_by,s_created_by) VALUES (?, ?, ?, ?, ?,?,?)"
          msgs = executeSql.ExecutereturnOne(sql, data)
          data1=(str(msgs),data[2],data[4],data[6])
          sql1="INSERT INTO tbl_log_data (n_issue_id,s_remark,s_status,s_created_by) values(?,?,?,?)"
          msgs1 = executeSql.ExecuteOne(sql1, data1)
          return msgs1
     except Exception as e:
          print("Error in save StateRecord query==========================", e)
          return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

def getissuesdata(data):
     try:
          if(data=='' or data=='null'):
               data=[]
               sql="SELECT n_issue_id,s_area,s_gtc_name,s_remark,s_status,s_created_by,dbo.[FUN_GET_ISSUE_RE_id](n_issue_id) as _new_remark from tbl_issue_data"
          else:
               data=[data]
               sql="SELECT n_issue_id,s_area,s_gtc_name,s_remark,s_status,s_created_by,dbo.[FUN_GET_ISSUE_RE_id](n_issue_id) as _new_remark from tbl_issue_data where s_area=?"
          # data=data
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getting project category Record query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
def getissuesById(id):
     try:
          sql="select * from tbl_issue_data where n_issue_id=?"
          data=[id]
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in geeting state by id query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
def deleteissues(id):
     try:
          sql="delete from tbl_issue_data where n_issue_id=?"
          data=[id]
          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in deleting state data query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')
def update_issues_data(data):
    try:
        data1=(data[1],data[2],data[3])
        sql = "UPDATE tbl_issue_data SET  s_status=?, s_created_by=? WHERE n_issue_id=?"
        print("SQL Query:", sql)
        print("Data Values:", data1)
        msgs = executeSql.ExecuteOne(sql, data1)
        data2=(data[3],data[0],data[1],data[2])
        sql1="INSERT INTO tbl_log_data (n_issue_id,s_remark,s_status,s_created_by) values(?,?,?,?)"
        msgs1 = executeSql.ExecuteOne(sql1, data2)
        return msgs1
    except Exception as e:
        print("Error in update issues data query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1021310')