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
          sql="select * FROM tbl_user_master WHERE s_user_id =%s and s_password =%s"
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
               msgs=executeSql.ExecuteReturnId(sql,data)
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
          sql = "INSERT INTO tbl_widget(n_data_modal_id,s_widget_name,s_widget_desc,s_index_name_or_query,n_component_id,s_script,s_customized_query,s_manual_script,n_datasource_id,s_perameter,n_widget_grp_id,s_style_column,s_head_obj,s_calculated_field,s_filters,s_comp_property) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
def getfilterData():
     try:

          sql="SELECT * from tbl_global_region"
          data=''
          results=executeSql.ExecuteAllNew(sql,data)
          return results
     except Exception as e:

          print("Error in get datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getfltById(id):
     try:

          sql="select * from tbl_global_region where n_global_seq=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:

          print("Error in getbyid datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')

def deletefltData(id):
     try:
          sql="delete from tbl_global_region where n_global_seq=%s"
          data={id}
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in delete datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')


def updatefltData(data):
     try:
          sql="UPDATE tbl_global_region SET s_field=%s,s_value=%s,s_type=%s,s_tbl_field_name=%s,s_other_table=%s,s_other_table_fields=%s WHERE n_global_seq=%s"
          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in update datasource ==============================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

def savefltDetail(data):
     try:
          sql="INSERT INTO tbl_global_region(s_field,s_value,s_type,s_tbl_field_name,s_other_table,s_other_table_fields) values(%s,%s,%s,%s,%s,%s)"

          results=executeSql.ExecuteOne(sql,data)
          return results
     except Exception as e:

          print("Error in insert datasource ==============================",e)
          return  make_response(middleware.exe_msgs(responses.update_501,str(e.args),'1021500'),500)



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