from cmath import nan
#import pdfplumber
import re
import itertools
# from xml.etree.ElementPath import ops
import pandas as pd
import json 
import time,os as ops,sys
import pandas as pd
from datetime import datetime,timedelta
from openpyxl import load_workbook
# from num2words import num2words
import numpy
import openpyxl
import math
import json
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart

global tr2 
# scripttype=sys.argv[1]
date1=datetime.today().strftime('%d-%m-%Y')
cuurent_datetime_here=datetime.now()
def make_date_time():
    return cuurent_datetime_here.strftime("%d.%m.%Y")

# def get_filter_doc(): 
#     # read new fbl_f file  
#     print(os.getcwd())
#     # df1 = pd.read_json(os.getcwd()+"\\test.json")
#     with open(os.getcwd()+'\\log\\logs_finance_data_'+date1+'.json','r') as f:
#         log_array=json.load(f) 
#     print(log_array)
    
    # global tr2
    # tr2=''
    # for k in log_array:   
    #     tr2+= '<tr><td>'+str(k["scroll_no"])+'</td> <td>'+str(k["massage"])+'</td> <td>'+str(k["Date"])+'</td></tr>'
    # send_email(tr2)


def send_email(data):
    sender_email = 'onm.planning@pipelineinfra.com'
    receiver_email = ['divakar.kunapareddy@pipelineinfra.com','venkateshwarreddy.gangula@pipelineinfra.com','rama.nallanichakravarthula@pipelineinfra.com','shaik.khan@pipelineinfra.com','ankur.mullick@pipelineinfra.com','malay.baiswar@pipelineinfra.com','narendra.swain@pipelineinfra.com','pavankumar.deshmukh@pipelineinfra.com','Vishal.Bhujbal@pipelineinfra.com','Nitin.Kharade@pipelineinfra.com','bhim.tomar@pipelineinfra.com','satya.srinivas@pipelineinfra.com','arti.sawant@pipelineinfra.com','ravikiran.gawde@pipelineinfra.com','Dipti.Lohchab@pipelineinfra.com','Madhu.Joshi@ril.com','ravindra.damaraju@pipelineinfra.com','santosh.bhise@pipelineinfra.com','naresh.kodavati@pipelineinfra.com','manas.bhaumik@pipelineinfra.com','navneet.saxena@pipelineinfra.com','indraganti.krishna@pipelineinfra.com','muthukumar.pandian@pipelineinfra.com','mukul.singhal@pipelineinfra.com','subrata.banerjee@pipelineinfra.com','suprakash.chattopadhyay@pipelineinfra.com','sharad.koparkar@pipelineinfra.com','soumyendra.mandal@pipelineinfra.com','swarup.roy@pipelineinfra.com','bishnu.mishra@pipelineinfra.com','sambasivarao.jaddu@pipelineinfra.com','prakash.kanchukatla@pipelineinfra.com','pil.oprscentrehyderabad@pipelineinfra.com','pil.oprscentremumbai@pipelineinfra.com','Saila.Babu@pipelineinfra.com','eswar.rao@pipelineinfra.com','rajesh.pakade@pipelineinfra.com','hari.n.prasad@pipelineinfra.com','dheeraj.m.matla@pipelineinfra.com','manoj.s.saxena@pipelineinfra.com','Nikhil.Mittal@pipelineinfra.com','komaljit.singh@pipelineinfra.com','Vishal.Bhujbal@pipelineinfra.com','Shraddha.Patil@pipelineinfra.com','radhey.singh@ril.com','rajeev.k.gupta@ril.com','Jashvant.Vadodaria@ril.com','Madhu.Joshi@ril.com','krishna.kotagiri@pipelineinfra.com','suresh.s.iyer@pipelineinfra.com','Parth.Patel@pipelineinfra.com','aniket.dey@pipelineinfra.com','amiya.pusker@pipelineinfra.com','Amit.Misal@pipelineinfra.com','Jalindar.Chavan@pipelineinfra.com']
    # cc_email = ['sanket.sagvekar@apponext.com']

    
    cc_email = ['pradeep.chauhan@pipelineinfra.com','anoop.naik@pipelineinfra.com','rpa.supportpil@pipelineinfra.com','Vidya.Dsouza@pipelineinfra.com','Rupali.Palkar@pipelineinfra.com']
    # receiver_email = ['sanket.sagvekar@apponext.com']
    to_email = ", ".join(receiver_email)
    cc_email1=", ".join(cc_email)
    # password = input("Type your password and press enter:")

    message = MIMEMultipart("alternative")
    message["Subject"] = data['pm_compliance']
    message["From"] = sender_email
    message["To"] = to_email
    message["Cc"] = cc_email1

    # Create the plain-text and HTML version of your message
    
    html = """
         <!doctype html>
    <html lang="en-US">

    <head>
        <meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
        <title>PIL Limited</title>
        <style type="text/css">
            a:hover {
                text-decoration: none !important;
            }

            :focus {
                outline: none;
                border: 0;
            }

            th, td {
    padding: 4px;
    /* text-align: center; */
    }
        </style>
    </head>

    <body marginheight="0" topmargin="0" marginwidth="0" style="margin: 0px; background-color: #f2f8f9;" bgcolor="#eaeeef"
        leftmargin="0">
        <!--100% body table-->
        <table cellspacing="0" border="0" cellpadding="0" width="100%" bgcolor="#f2f8f9"
            style="@import url(https://fonts.googleapis.com/css?family=Roboto:400,500,300); font-family: 'Roboto', sans-serif , Arial,  Helvetica, sans-serif;">
            <tr>
                <td>
                    <table style="background-color: #f2f8f9; max-width:670px; margin:0 auto;" width="100%" border="0"
                        align="center" cellpadding="0" cellspacing="0">
                        <tr>
                            <td height="40px;">&nbsp;</td>
                        </tr>
                        <tr>
                            <td>
                                <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                    style="max-width:630px; background:#fff; border-radius:3px; text-align:left; -webkit-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);-moz-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12)">
                                    <tr>
                                        <td style="padding:20px;">
                                            <table width="100%" border="0" align="center" cellpadding="0" cellspacing="0">
                                                <tr>
                                                    <td>
                                                        <h1
                                                            style="color: #3075BA; font-weight: 400; margin: 0; font-size: 16px;">
                                                            Dear All,

                                                        </h1>
                                                        <p
                                                            style="font-size:15px; color:#171f23de; line-height:24px; margin:8px 0 30px;">

Please find attached , """+data['pm_compliance']+"""

                                                        </p>
                                                        
                                                        
                                                      
                                                    <hr>
                                                    <p style="font-size:15px; color:#171f23de; line-height:24px;">
                                                        Summary
                                                    </p>
                                                    
                                        
                                                    <table width="100%" border="1" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                        <tr style="background-color: #455c70; font-weight: 900;text-align: center;color: white;">
                                                <th colspan="5" width='100%'>PM/ Preservation Orders</th>
                                            </tr>
                                                        <tr>
                                                            <th>Particulars</th>
                                                            <th>Target</th>
                                                            <th>MTD Comp.</th>
                                                            <th>YTD Comp.</th>
                                                            <th>Major Contributors for Low Comp.</th>
                                                        </tr><tr><td>Planned Orders Compliance</td><td>98%</td><td>"""+data['mtd_plan']+"""</td><td>"""+data['ytd_plan']+"""</td><td>"""+data['major_pl_unpr']+"""</td></tr><tr><td>Preservation Orders Compliance</td><td>98%</td><td>"""+data['mtd_pres']+"""</td><td>"""+data['ytd_pres']+"""</td><td>"""+data['major_pl_unpr1']+"""</td></tr><tr><td>PM Orders > 7 days</td><td>0</td><td>"""+str(data['mtd_pend_cnt_str'])+"""</td><td>"""+str(data['mtd_pend_cnt_str'])+"""</td><td>"""+data['major_pl_cs']+"""</td></tr><tr><td>Notifications > 30 days</td><td>0</td><td>0</td><td>"""+data['mtd_pend_cnt_noti']+"""</td><td>"""+data['pen_30_days']+"""</td></tr></table>
                                                        <p style="font-size:15px; color:#171f23de; line-height:24px;">
                                                            
                                                        </p>

                                                        <table width="100%" border="1" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                    <tr style="background-color: #455c70; font-weight: 900;text-align: center;color: white;">
                                                <th colspan="4" width='100%'>Short Closed/ Rescheduled Orders</th>
                                            </tr>                                    
                                                        <tr>
                                                            <th rowspan="2">Particulars</th>
                                                            <th colspan="2">Total</th>
                                                            <th rowspan="2">Major Contributors</th>
                                                        </tr>
                                                        <tr><td>MTD</td><td>YTD</td></tr><tr><td>Short Closed Orders</td><td>"""+str(data['mtd_sh_cld'])+"""</td><td>"""+str(data['ytd_sh_cld'])+"""</td><td>"""+data['major_pl_sch']+"""</td></tr><tr><td>Total Re-Scheduled Orders</td><td>"""+data['mon_ageing']+"""</td><td>"""+data['total_or_days']+"""</td><td>"""+data['ageing_tot']+"""</td></tr><tr><td>Calibration Orders Re-Scheduled* (Maint Activity Type - PC4)</td><td>"""+data['pc4'].split('_')[0]+"""</td><td>-</td><td>"""+data['pc4'].split('_')[1]+"""</td></tr></table>
                                                        <p style="font-size:15px; color:#171f23de; line-height:24px;font-weight:bold">
                                                            """+data['getpc_4']+""" 
                                                        </p>

                                                        <table width="100%" border="1" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                        <tr style="background-color: #455c70; font-weight: 900;text-align: center;color: white;">
                                                <th colspan="5" width='100%'>BOM Exceptions </th></tr>
                                                        <tr>
                                                            <th rowspan='2'>Discipline</th>
                                                            <th colspan="2">Items that are outside BOM </th>
                                                            <th colspan="2">Items that more than BOM qty</th>
                                                        </tr>
                                                        <tr><td>Planned</td><td>Unplanned</td><td>Planned</td><td>Unplanned</td></tr><tr><td>ELE/PIMS</td><td>"""+data['bmout_ele_pims']+"""</td><td>"""+data['bmout_pl_ele_pims']+"""</td><td>"""+data['bmqty_ele_pims']+"""</td><td>"""+data['bmqty_pl_ele_pims']+"""</td></tr><tr><td>INS</td><td>"""+data['bmout_ins']+"""</td><td>"""+data['bmout_pl_ins']+"""</td><td>"""+data['bmqty_ins']+"""</td><td>"""+data['bmqty_pl_ins']+"""</td></tr><tr><td>IPA</td><td>"""+data['bmout_ipa']+"""</td><td>"""+data['bmout_pl_ipa']+"""</td><td>"""+data['bmqty_ipa']+"""</td><td>"""+data['bmqty_pl_ipa']+"""</td></tr><tr><td>MECH</td><td>"""+data['bmout_MECH']+"""</td><td>"""+data['bmout_pl_MECH']+"""</td><td>"""+data['bmqty_MECH']+"""</td><td>"""+data['bmqty_pl_MECH']+"""</td></tr><tr><td>TOTAL</td><td>"""+data['bmout_tot']+"""</td><td>"""+data['bmout_pl_tot']+"""</td><td>"""+data['bmqty_tot']+"""</td><td>"""+data['bmqty_pl_tot']+"""</td></tr></table>
                                                        <p style="font-size:15px; color:#171f23de; line-height:24px;">
                                                            
                                                        </p>

                                                        <table width="100%" border="1" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                        <tr style="background-color: #455c70; font-weight: 900;text-align: center;color: white;">
                                                <th colspan="4" width='100%'>PM Work Confirmations </th></tr>
                                                        <tr>
                                                            <th>Particulars</th>
                                                            <th >%Non-Compliance</th>
                                                            <th >Major Contributors for Non-Compliance</th>
                                                        </tr>
                                                        <tr><td>Inspection done/Job Completed </td><td>"""+str(data['a'])+"""%</td><td>-</td></tr><tr><td>PM done/completed & Checked/Found ok</td><td>"""+str(data['b'])+"""%</td><td>"""+data['b_4'] +"""</td></tr><tr><td>Name of PM mentioned</td><td>"""+str(data['c'])+"""%</td><td>-</td></tr><tr><td>Total Non-Compliance </td><td colspan="2" style="text-align:center;">"""+data['pm_non_cmp_per']+"""</td></tr></table>
                                                                                                 <p style="font-size:15px; color:#171f23de; line-height:24px;">
                                                            
                                                        </p>

                                                        <table width="100%" border="1" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                        <tr style="background-color: #455c70; font-weight: 900;text-align: center;color: white;">
                                                <th colspan="5" width='100%'>"""+data['totalid']+"""</th></tr>
                                                        <tr>
                                                            <th>No. of orders</th>
                                                            <th >Total Value (INR)</th>
                                                             <th >Booking Status</th>
                                                            <th >Major Contributors (Incorrect Bookings)</th>
                                                        </tr>
                                                        <tr>
                                                        <td>1</td>
                                                        <td>2</td>
                                                        <td>631.51</td>
                                                        <td>Incorrect</td>
                                                        <td>CS01,CS03</td></tr></table>
                                                </td>
                                            </tr>
                                            
                                            <tr>
                                                <td style="height:25px;">
                                                    <p style="font-size:15px; color:#171f23de; line-height:24px;">
                                                    <b>Regards,</b><br>

<b>ONM Planning</b><br>
<b><img src='https://www.pipelineinfra.com/sites/default/files/pil%20logo.png'></img></b><br>

<b>Pipeline Infrastructure Limited</b><br>

<b>Seawoods Grand Central, Tower-1, 3rd Level</b><br>

<b>C Wing - 301 to 304, Sec 40, Seawoods Railway Station,</b><br>

<b>Navi Mumbai, Thane,Maharashtra – 400706</b><br>

<b><a href='www.pipelineinfra.com'>www.pipelineinfra.com</a></b><br>
                                                        <b><img src='https://www.pipelineinfra.com/sites/default/files/gva-sliderlayer-upload/GPTW.jpg' width='50' height='60'></img></b><br>
                                                        <b><a href='https://www.linkedin.com/company/pipeline-infrastructure-limited/'>Follow us https://www.linkedin.com/company/pipeline-infrastructure-limited/</a></b>
                                                       
                                                    </p>
                                                    
                                                </td>
                                            </tr>

                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="height:80px;">&nbsp;</td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    <!--/100% body table-->
    </body>
    </html>
    """
     # Turn these into plain/html MIMEText objects
    # part1 = MIMEText(text, "plain")


    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    # message.attach(part1)
    part2 = MIMEText(html, "html")
    message.attach(part2)
    files=[]
    filepath1="E:\\bi-vertib_18_11_2021\\bi-vertib\\PM_compliance_SEP_2023_compressed.pdf"
    file_exists = filepath1
    # if file_exists == True:
    files.append(filepath1)
    # files.append(filepath1)
        # filepath2=ops.getcwd()+"\\shahgung_to_csmt.pdf"
        # filepath3=ops.getcwd()+"\\Signature document.pdf"
        

    for a_file in files:
        attachment = open(a_file, 'rb')
        file_name = ops.path.basename(a_file)
        part = MIMEBase('application','octet-stream')
        part.set_payload(attachment.read())
        part.add_header('Content-Disposition',
                        'attachment',
                        filename=file_name)
        encoders.encode_base64(part)
        message.attach(part)
    text = message.as_string()
    # Create secure connection with server and send email
    # context = ssl.create_default_context()
    with smtplib.SMTP('172.16.10.20', 25) as server:
        # server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, text
        )
# get_filter_doc()
# send_email()
# <tr><td>"""+data['ord_cnt']+"""</td><td>"""+data['tot_amt']+"""</td><td>"""+data['status']+"""</td><td>"""+data['remarks']+"""</td></tr>