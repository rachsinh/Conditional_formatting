from flask import make_response
import os as ops
# from route import app
#import pdfkit 
from Execute import responses,middleware
import platform
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime,timedelta

class createPdf:
    try:
        def __init__(self):
            #print("aaa")
            if platform.system()=='Windows':
                if ops.getcwd() !="E:\\bi-vertib_18_11_2021\\bi-vertib\\phantomjs-2.1.1-windows\\bin":
                    #ops.chdir(ops.getcwd()+"\\phantomjs-2.1.1-windows\\bin")
                    print('dds')

            elif platform.system()=='Linux':
                ops.chdir(ops.getcwd()+"\\phantomjs-2.1.1-linux-x86_64\\bin")
                
        def generatePdf(self,data):
            try:
                #url.replace(" ", "")
                #flt=json.loads(flt)
                # app = Flask(__name__)
                # with app.app_context():
                    # within this block, current_app points to app.
                    # print(current_app.name)
                    # session['username'] = data["user"]
                if data['type']=='JN':
                    siteurl='http://10.11.48.28:5001/join_ticket/perams?station_nm='+data['s_station_id']+'&date='+data['current_date']
                    filename=ops.getcwd()+"\\static\\dailyreport\\"+data['gas_day']+"\\E-Joint Ticket for "+data['s_pgts_station_nm']+"-"+datetime.strptime(data['gas_day'], '%d-%m-%Y').strftime('%d %b %Y')
                elif data['type']=='FN':
                    siteurl='http://10.11.48.28:5001/forthnight_ticket/perams?station_nm='+data['s_station_id']+'&date='+data['current_date']
                    filename=ops.getcwd()+"\\static\\dailyreport\\"+data['gas_day']+"\\E-Joint Ticket "+data['s_pgts_station_nm']+"_"+data['forth_type']+" "+data['type']+" -"+datetime.strptime(data['gas_day'], '%d-%m-%Y').strftime('%b %Y')
                # url="http://10.11.48.28:5001/emailhtml#$dash="+data["dashid"]+";user="+data["user"]+";filter="+flt
                # url=url.replace(" ","~")
                # url=url.replace('"',"^")
                print(siteurl)
                #url="http://10.11.48.28:5001/main#/emailHtml/$dash="+data[1]+";filter="+flt
                #process =ops.system('phantomjs rasterize.js http://10.11.48.28:5001/main#/emailHtml/$dash='+data[1]+';user='+data[5]+';filter='+flt+ ' '+data[1]+'_'+data[5]+'.pdf')
                # filename=ops.getcwd()+"\\static\\dailyreport\\"+data['gas_day']+"\\E-Joint Ticket for "+data['s_pgts_station_nm']+"-"+datetime.strptime(data['gas_day'], '%d-%m-%Y').strftime('%d %b %Y')
                process =ops.system('cd phantomjs-2.1.1-windows\\bin && phantomjs rasterize.js "'+siteurl+'" "'+filename+'.pdf"')
                return process
                #print(data)
            
            except Exception as e:
                print("Error in executing scheduler=================================", e)
                return responses.shedulerErr

    except Exception as e:
            print("Error in creating pdf=================================", e)
            

def mailcontent(reqdata,data):
    try:
        # print(data)
        email_user = 'pil.measurementdesk@pipelineinfra.com'
        #email_password = 'Apponextsystems@12345'
        emailreceipt=data[0]['to_email']+data[0]['cc_email']
        RECIPIENT_LIST=emailreceipt.split(',')#['suprakash.chattopadhyay@pipelineinfra.com','vanashree.mhatre@pipelineinfra.com','sanket.sagvekar@apponext.com','shikha.dwivedi@apponext.com','abhishek.thakur@apponext.com','shailendra.tiwari@pipelineinfra.com','vidya.dsouza@pipelineinfra.com','amit.verma@pipelineinfra.com']#data[0]['to_email']
        msg = MIMEMultipart()
        msg['From'] = 'pil.measurementdesk@pipelineinfra.com' #email_user
        msg['To'] = data[0]['to_email']#'suprakash.chattopadhyay@pipelineinfra.com,vanashree.mhatre@pipelineinfra.com'#data[0]['to_email']
        msg['Cc'] =data[0]['cc_email']#'shikha.dwivedi@apponext.com,shailendra.tiwari@pipelineinfra.com,vidya.dsouza@pipelineinfra.com,amit.verma@pipelineinfra.com,abhishek.thakur@apponext.com,sanket.sagvekar@apponext.com'#data[0]['cc_email']
        #email_send
        msg['Subject'] = "e-Joint Ticket for "+reqdata['s_pgts_station_nm']+" "+datetime.strptime(reqdata['gas_day'], '%d-%m-%Y').strftime('%d %b %Y')
        if(reqdata['s_pgts_station_nm']=='SG1'):
            datatype='Received'
        else:
            datatype='delivered'

        html="""
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
                    <table style="background-color: #f2f8f9; max-width:750px; margin:0 auto;" width="100%" border="0"
                        align="center" cellpadding="0" cellspacing="0">
                        <tr>
                            <td height="40px;">&nbsp;</td>
                        </tr>
                        <tr>
                            <td>
                                <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                    style="max-width:750px; background:#fff; border-radius:3px; text-align:left; -webkit-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);-moz-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12)">
                                    <tr>
                                        <td style="padding:20px;">
                                            <table width="100%" border="0" align="center" cellpadding="0" cellspacing="0">
                                                <tr>
                                                    <td>
                                                        <h1
                                                            style="color: #171f23de; font-weight: 400; margin: 0; font-size: 15px;">
                                                            Dear Sir,
                                                        </h1>
                                                        <p
                                                            style="font-size:15px; color:#171f23de; line-height:24px; margin:8px 0 30px;">
                                                           Please find attached herewith the e-joint ticket for gas """+datatype+""" to """+reqdata['s_pgts_station_nm']+""". Necessary flow computer report has been attached herewith for your reference.
                                                        </p>
                                                        <p
                                                            style="font-size:15px; color:#171f23de; line-height:24px; margin:8px 0 30px;">
                                                            Kindly review the quantities in the joint ticket and in case of any discrepancy, same to be reverted to us within 4 to 6 Hrs. of this email; else the quantities indicated in the joint ticket shall be deemed acceptable.
                                                        </p>
                                                       
                                                    <table width="100%" border="0" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                 
                                            <tr>
                                                <td style="height:25px;">
                                                    <p style="font-size:12px; color:#171f23de; line-height:24px;">
                                                        <b style="color:#171f23de;">Thanks & Regards,</b> <br>
                                                        <b style="color:#171f23de;">Measurement & Control Centre</b><br>
                                                        <b><img src='https://www.pipelineinfra.com/sites/default/files/pil%20logo.png'></img></b></br>
                                                        <b>Seawoods Grand Central</b><br>
                                                        <b>Tower-1, 3rd Level, C Wing - 301 to 304,</b><br>
                                                        <b>Plot R1, Sector 40, Seawoods Railway Station,</b><br>
                                                        <b>Navi Mumbai - 400706</b><br>
                                                        <b>Maharashtra</b><br>
                                                        <b>Land Line:- 02235018010/ 02235018011</b><br>
                                                        <b>Mob:- 8828034824</b><br>
                                                        <b>Tollfree no: 18001033399</b><br>
                                                        <b>Emergency no: 022-35018016 </b><br></br>
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
    </body>
    </html>"""
        part2 = MIMEText(html, "html")
        files=[]
        msg.attach(part2)
        cpdf=createPdf()
        pdfres=cpdf.generatePdf(reqdata)
        # ops.chdir("E:\\bi-vertib_18_11_2021\\bi-vertib")
        for filename in data:
            if filename['s_station_type']=='GC':
                #ops.getcwd()+"\\static\\dailyreport
                filepath="E:\\Reports\\"+reqdata['gas_day']+"\\"+filename['s_station_name']+"_"+filename['s_station_no']+"_GC_24_hours_Avg_report_"+reqdata['gas_day']+".pdf"
                
                file_exists = ops.path.exists(filepath)
                if file_exists == True:
                    files.append(filepath)
            elif filename['s_station_type']=='FC':
                if filename['s_sub_station_type'] == "FC":
                    if '101A' in filename["s_station_no"] or '101B' in filename["s_station_no"]:
                        print('Flowstation File get')
                    else:
                        filepath="E:\\Reports\\"+reqdata['gas_day']+"\\"+filename["s_station_name"]+"_"+filename["s_station_no"]+"_Daily_Report_"+reqdata['gas_day']+".pdf"
                        file_exists = ops.path.exists(filepath)
                        if file_exists == True:
                            files.append(filepath)
                     
                elif filename["s_sub_station_type"] == 'CGD':
                    filepath="E:\\Reports\\"+reqdata['gas_day']+"\\"+filename['s_station_name'].replace(' ','_')+"_"+filename['s_meter_name_number'].replace(' ','_')+"_"+filename['s_station_no'].replace(' ','_')+"_"+datetime.strptime(reqdata['current_date'], '%Y-%m-%d').strftime('%d-%m-%Y')+".pdf"
                    file_exists = ops.path.exists(filepath)
                    if file_exists == True:
                        files.append(filepath)
                    
                # filepath=ops.getcwd()+"\\static\\dailyreport\\"+filename['s_station_name']+"_"+filename['s_station_no']+"_GC_24_hours_Avg_report_"+reqdata[2]+".pdf"
        filepath1=ops.getcwd()+"\\static\\dailyreport\\"+reqdata['gas_day']+"\\E-Joint Ticket for "+reqdata['s_pgts_station_nm']+"-"+datetime.strptime(reqdata['gas_day'], '%d-%m-%Y').strftime('%d %b %Y')+".pdf"
        file_exists = ops.path.exists(filepath1)
        if file_exists == True:
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
            msg.attach(part)

        text = msg.as_string()
        # server = smtplib.SMTP('172.16.10.20', 25)
        # server.starttls()
        # server.login(email_user,email_password)
        with smtplib.SMTP('172.16.10.20', 25) as server:
        #server.sendmail(email_user,RECIPIENT_LIST,text)
            server.sendmail(email_user,RECIPIENT_LIST,text)
        # server.quit()

    except Exception as e:
        print("Error in sending mail=================================", e)
        return  make_response(middleware.exe_msgs(responses.getById_501,str(e.args),'1023500'),500)

def fortntcontent(reqdata,data):
    try:
        # print(data)
        email_user = 'pil.measurementdesk@pipelineinfra.com'
        #email_password = 'Apponextsystems@12345'
        emailreceipt=data[0]['to_email']+data[0]['cc_email']
        RECIPIENT_LIST=emailreceipt.split(',')#['suprakash.chattopadhyay@pipelineinfra.com','vanashree.mhatre@pipelineinfra.com','sanket.sagvekar@apponext.com','shikha.dwivedi@apponext.com','abhishek.thakur@apponext.com','shailendra.tiwari@pipelineinfra.com','vidya.dsouza@pipelineinfra.com','amit.verma@pipelineinfra.com']#data[0]['to_email']
        msg = MIMEMultipart()
        msg['From'] = 'pil.measurementdesk@pipelineinfra.com' #email_user
        msg['To'] = data[0]['to_email']#'suprakash.chattopadhyay@pipelineinfra.com,vanashree.mhatre@pipelineinfra.com'#data[0]['to_email']
        msg['Cc'] =data[0]['cc_email']#'shikha.dwivedi@apponext.com,shailendra.tiwari@pipelineinfra.com,vidya.dsouza@pipelineinfra.com,amit.verma@pipelineinfra.com,abhishek.thakur@apponext.com,sanket.sagvekar@apponext.com'#data[0]['cc_email']        
        #email_send
        msg['Subject'] = "e-Joint Ticket for "+reqdata['s_pgts_station_nm']+"_"+reqdata['forth_type']+" "+reqdata['type']+" "+datetime.strptime(reqdata['gas_day'], '%d-%m-%Y').strftime(' %b %Y')
        if(reqdata['s_pgts_station_nm']=='SG1'):
            datatype='Received'
        else:
            datatype='delivered'
        html="""
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
                    <table style="background-color: #f2f8f9; max-width:750px; margin:0 auto;" width="100%" border="0"
                        align="center" cellpadding="0" cellspacing="0">
                        <tr>
                            <td height="40px;">&nbsp;</td>
                        </tr>
                        <tr>
                            <td>
                                <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                    style="max-width:750px; background:#fff; border-radius:3px; text-align:left; -webkit-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);-moz-box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12);box-shadow:0 1px 3px 0 rgba(0, 0, 0, 0.16), 0 1px 3px 0 rgba(0, 0, 0, 0.12)">
                                    <tr>
                                        <td style="padding:20px;">
                                            <table width="100%" border="0" align="center" cellpadding="0" cellspacing="0">
                                                <tr>
                                                    <td>
                                                        <h1
                                                            style="color: #171f23de; font-weight: 400; margin: 0; font-size: 15px;">
                                                            Dear Sir,
                                                        </h1>
                                                        <p
                                                            style="font-size:15px; color:#171f23de; line-height:24px; margin:8px 0 30px;">
                                                           Please find attached herewith the e-joint ticket for gas """+datatype+""" to """+reqdata['s_pgts_station_nm']+""" for """+reqdata['forth_type']+""" """+reqdata['type']+""" """+datetime.strptime(reqdata['gas_day'], '%d-%m-%Y').strftime(' %b %Y')+""". Kindly review the quantities in the joint ticket & send us the signed copy of the same.
                                                        </p>
                                                        <p
                                                            style="font-size:15px; color:#171f23de; line-height:24px; margin:8px 0 30px;">
                                                            In case of any discrepancy, same to be reverted back to us within 6 Hrs. of this email; else the quantities indicated in the joint ticket shall be deemed acceptable.
                                                        </p>
                                                       
                                                    <table width="100%" border="0" align="center" cellpadding="0"
                                                        cellspacing="0" style=" border-collapse: collapse;">
                                                 
                                            <tr>
                                                <td style="height:25px;">
                                                    <p style="font-size:12px; color:#171f23de; line-height:24px;">
                                                        <b style="color:#171f23de;">Thanks & Regards,</b> <br>
                                                        <b style="color:#171f23de;">Measurement & Control Centre</b><br>
                                                        <b><img src='https://www.pipelineinfra.com/sites/default/files/pil%20logo.png'></img></b></br>
                                                        <b>Seawoods Grand Central</b><br>
                                                        <b>Tower-1, 3rd Level, C Wing - 301 to 304,</b><br>
                                                        <b>Plot R1, Sector 40, Seawoods Railway Station,</b><br>
                                                        <b>Navi Mumbai - 400706</b><br>
                                                        <b>Maharashtra</b><br>
                                                        <b>Land Line:- 02235018010/ 02235018011</b><br>
                                                        <b>Mob:- 8828034824</b><br>
                                                        <b>Tollfree no: 18001033399</b><br>
                                                        <b>Emergency no: 022-35018016 </b><br></br>
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
    </body>
    </html>"""
        part2 = MIMEText(html, "html")
        files=[]
        msg.attach(part2)
        cpdf=createPdf()
        pdfres=cpdf.generatePdf(reqdata)
        # ops.chdir("E:\\bi-vertib_18_11_2021\\bi-vertib")
        # for filename in data:
        #     if filename['s_station_type']=='GC':
        #         filepath=ops.getcwd()+"\\static\\dailyreport\\"+reqdata['gas_day']+"\\"+filename['s_station_name']+"_"+filename['s_station_no']+"_GC_24_hours_Avg_report_"+reqdata['gas_day']+".pdf"
        #         files.append(filepath)
        #     elif filename['s_station_type']=='FC':
        #         if filename['s_sub_station_type'] == "FC":
        #             if '101A' in filename["s_station_no"] or '101B' in filename["s_station_no"]:
        #                 print('not')
        #             else:
        #                 filepath=ops.getcwd()+"\\static\\dailyreport\\"+reqdata['gas_day']+"\\"+filename["s_station_name"]+"_"+filename["s_station_no"]+"_Daily_Report_"+reqdata['gas_day']+".pdf"
        #                 files.append(filepath)
                     
        #         elif filename["s_sub_station_type"] == 'CGD':
        #             filepath=ops.getcwd()+"\\static\\dailyreport\\"+reqdata['gas_day']+"\\"+filename['s_station_name'].replace(' ','_')+"_"+filename['s_meter_name_number'].replace(' ','_')+"_"+filename['s_station_no'].replace(' ','_')+"_"+datetime.strptime(reqdata['current_date'], '%Y-%m-%d').strftime('%d-%m-%Y')+".pdf"
        #             files.append(filepath)
                    
                # filepath=ops.getcwd()+"\\static\\dailyreport\\"+filename['s_station_name']+"_"+filename['s_station_no']+"_GC_24_hours_Avg_report_"+reqdata[2]+".pdf"
        filepath1=ops.getcwd()+"\\static\\dailyreport\\"+reqdata['gas_day']+"\\E-Joint Ticket "+reqdata['s_pgts_station_nm']+"_"+reqdata['forth_type']+" "+reqdata['type']+" -"+datetime.strptime(reqdata['gas_day'], '%d-%m-%Y').strftime('%b %Y')+".pdf"
        files.append(filepath1)
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
            msg.attach(part)

        text = msg.as_string()
        # server = smtplib.SMTP('172.16.10.20', 25)
        # server.starttls()
        # server.login(email_user,email_password)
        with smtplib.SMTP('172.16.10.20', 25) as server:
        #server.sendmail(email_user,RECIPIENT_LIST,text)
            server.sendmail(email_user,RECIPIENT_LIST,text)
        # server.quit()

    except Exception as e:
        print("Error in sending fothnight mail=================================", e)
        return  make_response(middleware.exe_msgs(responses.getById_501,str(e.args),'1023500'),500)


#mailcontent()