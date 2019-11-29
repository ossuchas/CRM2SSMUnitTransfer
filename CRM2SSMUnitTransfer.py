from sqlalchemy import create_engine
import urllib
import sqlalchemy
import pandas as pd
import sys
import pyodbc
import logging
import glob
import smtplib
import os.path
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# for Logging
import socket

# for Logging
def get_ipaddr():
    try:
        host_name = socket.gethostname()
        return socket.gethostbyname(host_name)
    except:
        return "Unable to get Hostname and IP"


class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        # self._connection = pyodbc.connect(
        #     'Driver={SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
        self._connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
        self._cursor = self._connection.cursor()

    def query(self, query):
        try:
            result = self._cursor.execute(query)
        except Exception as e:
            logging.error('error execting query "{}", error: {}'.format(query, e))
            return None
        finally:
            return result

    def update(self, sqlStatement):
        try:
            self._cursor.execute(sqlStatement)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_sp(self, sqlStatement, params):
        try:
            self._cursor.execute(sqlStatement, params)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_spRet(self, sqlStatement, params):
        try:
            result = self._cursor.execute(sqlStatement, params)
        except Exception as e:
            print('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            return result

    def __del__(self):
        self._cursor.close()


def getDfltParam():
    """
    index value
    0 = Excel File Name
    1 = receivers ;
    2 = Subject Mail
    3 = Body Mail
    4 = Footer Mail
    5 = Log Path
    """

    strSQL = """
    SELECT long_desc
    FROM dbo.CRM_Param
    WHERE param_code = 'CRM_SSM_TRNS_XLS'
    ORDER BY param_seqn
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.long_desc)

    return returnVal


def send_email(subject, message, from_email, to_email=[], attachment=[]):
    """
    :param subject: email subject
    :param message: Body content of the email (string), can be HTML/CSS or plain text
    :param from_email: Email address from where the email is sent
    :param to_email: List of email recipients, example: ["a@a.com", "b@b.com"]
    :param attachment: List of attachments, exmaple: ["file1.txt", "file2.txt"]
    """
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ", ".join(to_email)
    msg.attach(MIMEText(message, 'html'))

    for f in attachment:
        with open(f, 'rb') as a_file:
            basename = os.path.basename(f)
            part = MIMEApplication(a_file.read(), Name=basename)

        part['Content-Disposition'] = 'attachment; filename="%s"' % basename
        msg.attach(part)

    # email = smtplib.SMTP('aphubtran01.ap-thai.com', 25)
    email = smtplib.SMTP('apmail.apthai.com', 25)
    email.sendmail(from_email, to_email, msg.as_string())
    email.quit()
    return;


def deleteXLSFile():
    filelist = glob.glob(os.path.join(".", "*.xlsx"))
    for f in filelist:
        os.remove(f)


def main(dfltVal):
    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    str_sql = """
    SELECT a.ProductID,
       a.SAPProductID,
       a.Project,
       a.ProjectType,
       a.UnitAmount AS TotalUnit,
	   (
	   SELECT COUNT(*)
FROM [dbo].[ICON_EntForms_Transfer] TR
    LEFT JOIN [dbo].[ICON_EntForms_Agreement] Arg
        ON Arg.ContractNumber = TR.ContractNumber
    LEFT JOIN [dbo].[ICON_EntForms_Products] P
        ON P.ProductID = Arg.ProductID
WHERE 1 = 1
      AND TR.TransferDateApprove IS NOT NULL
      AND P.RTPExcusive = '1'
	  AND P.ProductID = a.ProductID
	  )  AS TransferTotalUnit
FROM
(
    SELECT ProductID,
           Project,
           P.ProjectType,
           P.SAPProductID,
           (
               SELECT COUNT(*)
               FROM dbo.ICON_EntForms_Unit
               WHERE ProductID = P.ProductID
                     AND ISNULL(AssetType, 0) <> 4
                     AND ISNULL(AssetType, 0) <> 5
           ) AS UnitAmount,
           (
               SELECT COUNT(*)
               FROM dbo.ICON_EntForms_Booking
               WHERE ProductID = P.ProductID
                     AND CancelDate IS NULL
           ) AS BookAmount
    FROM dbo.ICON_EntForms_Products P
    WHERE Producttype IN ( 'โครงการแนวราบ', 'โครงการแนวสูง' )
          AND RTPExcusive = '1'
) AS a
WHERE a.UnitAmount <> 0
ORDER BY a.ProductID;
    """

    # Setup Format File Name CSV
    date_fmt = datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = dfltVal[0]
    file_type = ".xlsx"
    full_file_name = "{}{}{}".format(file_name, date_fmt, file_type)
    logging.info("File Name => {}".format(full_file_name))

    df = pd.read_sql(sql=str_sql, con=db)

    # Read by SQL Statement
    logging.info("<<<Before Read SQL to Excel File>>>")
    df.to_excel(full_file_name, index=None, header=True)
    logging.info("<<<After Read SQL to Excel File>>>")

    # Prepare Send Mail
    last_month = datetime.now() - relativedelta(months=1)
    tx_last_month = format(last_month, '%B %Y')

    logging.info("Send Mail Start")
    sender = 'no-reply@apthai.com'
    receivers = dfltVal[1].split(';')

    subject = "{} ({})".format(dfltVal[2], datetime.now().strftime("%d/%m/%Y"))
    bodyMsg_tmp = dfltVal[3].replace("PERIOD_MONTH", datetime.now().strftime("%d/%m/%Y"))
    bodyMsg = "{}{}".format(bodyMsg_tmp, dfltVal[4])

    attachedFile = [full_file_name]

    send_email(subject, bodyMsg, sender, receivers, attachedFile)
    logging.info("Successfully sent email")


if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    log_path = dfltVal[5]
    logFile = log_path + '/CRM2SSMUnitTransfer.log'

    APPNAME='CRM2SSMUnitTransfer'
    IPADDR=get_ipaddr()
    FORMAT="%(asctime)-5s {} {}: [%(levelname)-8s] >> %(message)s".format(IPADDR, APPNAME)

    logging.basicConfig(level=logging.DEBUG,
                        format=FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(dfltVal)

    # Delete Excel File in current path execution
    logging.info('Delete Execl File')
    deleteXLSFile()
    logging.info('End Process')
    logging.debug('#####################')
