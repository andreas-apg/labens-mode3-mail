#!/usr/bin/python3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import jinja2
import envios
import settings

def sendMailHTML(server,port,user,password,to,subject,html):
    msg = MIMEMultipart()
    msg['From'] = user
    msg['Subject'] = subject

    msg.attach(MIMEText(html,'html','utf-8'))

    smtp = smtplib.SMTP_SSL(server,port)
    smtp.login(user,password)
    smtp.sendmail(user,to,msg.as_string())

    smtp.quit()


def renderHTML(fileName,content):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates/'))
    template = env.get_template(fileName)
    output = template.render(content)

    return output

def getMailAddresses():
    mailAddresses = []

    #Provisoriamente, os emais são salvos no arquivo mail.txt
    with open('mails.txt','r') as mailsFile:
        for mail in mailsFile:
            if not mail == '' and not mail == '\n':
                mailAddresses.append(mail.split('\n')[0])

    return mailAddresses

def main():
    files = envios.checkDB2(settings.DBPath+'/database.db')

    if not files == []:
        mailAddresses = getMailAddresses()

        if not mailAddresses == []:

            msgHtml = renderHTML('email-prob-com.html',{'tabelas':files})

            sendMailHTML(settings.mailServer['serverAddress'],settings.mailServer['serverPort'],settings.mailServer['user'],settings.mailServer['passwd'],mailAddresses,'Monitoramento EPESOLs',msgHtml)

if __name__ == '__main__':
    main()
