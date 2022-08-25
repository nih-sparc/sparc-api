import logging
from string import Template

import boto3
import sendgrid
from app.config import Config
from sendgrid.helpers.mail import Content, Email, Mail, To, Attachment, FileName, FileType, Disposition, FileContent

subject = "Message from SPARC Portal"

ses_client = boto3.client(
    "ses",
    aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
    aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
    region_name=Config.AWS_REGION,
)

sg_client = sendgrid.SendGridAPIClient(api_key=Config.SENDGRID_API_KEY)

feedback_email = Template('''\
<b>Thank you for your feedback</b><br>
<br>
Your message:<br>
<br>
$message
''')
issue_reporting_email = Template('''\
<b>You reported an issue on the SPARC Portal</b><br>
<br>
Provided data:<br>
<br>
$message
''')

community_spotlight_submit_form_email = Template('''\
<b>Request to create a $subject</b>
<br>
<b>Requestor's Contact Info</b>
<br>
<b>Name:</b>
<br>
$name
<br>
<b>E-mail:</b>
<br>
$email
<br>
<b>Community Spotlight Details</b>
<br>
<b>Title:</b><br>
$title
<br>
<b>Summary/details:</b><br>
$summary
<br>
<b>Supporting Info Url:</b><br>
$url
<br>
''')

news_and_events_submit_form_email = Template('''\
<b>Request to create a $subject</b>
<br>
<b>Requestor's Contact Info</b>
<br>
<b>Name:</b>
<br>
$name
<br>
<b>E-mail:</b><br>
$email
<br>
<b>News or Event Details</b>
<br>
<b>Title:</b><br>
$title
<br>
<b>Summary/details:</b><br>
$summary
<br>
<b>Supporting Info Url:</b><br>
$url
<br>
<b>Event specific details</b>
<br>
<b>Location:</b><br>
$location
<br>
<b>Date:</b><br>
$date
<br>
''')



class EmailSender(object):
    def __init__(self):
        self.default_subject = "Message from SPARC Portal"
        self.charset = "UTF-8"
        self.ses_sender = Config.SES_SENDER
        self.ses_arn = Config.SES_ARN

    def send_email(self, name, email_address, message):
        body = name + "\n" + email_address + "\n" + message
        ses_client.send_email(
            Source=self.ses_sender,
            Destination={"ToAddresses": [self.ses_sender]},
            Message={
                "Subject": {"Charset": self.charset, "Data": self.default_subject},
                "Body": {"Text": {"Charset": self.charset, "Data": body}},
            },
            SourceArn=self.ses_arn,
        )
    
    def sendgrid_email_with_attachment(self, fromm, to, subject, body, encoded_file, file_name, file_type):
        mail = Mail(
            Email(fromm),
            To(to),
            subject,
            Content("text/html", body)
        )
        attachedFile = Attachment(
            FileContent(encoded_file),
            FileName(file_name),
            FileType(file_type),
            Disposition('attachment')
        )
        logging.info('AttachedFile = ', attachedFile)

        mail.attachment = attachedFile

        logging.info('MAIL = ', mail)

        response = sg_client.send(mail)
        logging.info(f"Sending a '{subject}' mail with attachment using SendGrid")
        logging.debug(f"Mail to {to} response\nStatus code: {response.status_code}\n{response.body}")
        return response

    def sendgrid_email(self, fromm, to, subject, body):
        mail = Mail(
            Email(fromm),
            To(to),
            subject,
            Content("text/html", body)
        )
        response = sg_client.send(mail)
        logging.info(f"Sending a '{subject}' mail using SendGrid")
        logging.debug(f"Mail to {to} response\nStatus code: {response.status_code}\n{response.body}")
        return response
