import logging
from string import Template
import boto3
import sendgrid
from app.config import Config
from sendgrid.helpers.mail import Asm, Content, Email, Mail, To, Attachment, FileName, FileType, Disposition, \
    FileContent, GroupId, GroupsToDisplay

subject = "Message from SPARC Portal"

ses_client = boto3.client(
    "ses",
    aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
    aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
    region_name=Config.AWS_REGION,
)

sg_client = sendgrid.SendGridAPIClient(api_key=Config.SENDGRID_API_KEY)

feedback_email = Template('''\
<b>Thank you for your feedback on the SPARC Portal!</b>
<br>
<br>
Your message:
<br>
<br>
$message
''')

issue_reporting_email = Template('''\
<b>Thank you for reporting the following error/issue on the SPARC Portal!</b>
<br>
<br>
$message
''')

service_interest_email = Template('''\
<b>Thank you for expressing interest in a SPARC service! We have received your request and will be in contact as soon as possible.</b>
<br>
<br>
Your message:
<br>
<br>
$message
''')


general_interest_email = Template('''\
<b>Thank you for your submission to SPARC! We have received your question/inquiry and will be in contact as soon as possible.</b>
<br>
<br>
Your message:
<br>
<br>
$message
''')

creation_request_confirmation_email = Template('''\
<b>Thank you for the following SPARC submission! We have received your request and will be in contact as soon as possible.</b>
<br>
<br>
Your submission:
<br>
<br>
$message
''')

class EmailSender(object):
    def __init__(self):
        self.default_subject = "Message from SPARC Portal"
        self.charset = "UTF-8"
        self.ses_sender = Config.SES_SENDER
        self.ses_arn = Config.SES_ARN
        self.unsubscribe_group = 0  # Note that this must be an integer for use in "sendgrid.GoupId"
        if Config.SENDGRID_MONTHLY_STATS_UNSUBSCRIBE_GROUP != '':
            self.unsubscribe_group = int(Config.SENDGRID_MONTHLY_STATS_UNSUBSCRIBE_GROUP)

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
        mail.attachment = attachedFile

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

    def sendgrid_email_with_unsubscribe_group(self, fromm, to, subject, body):
        mail = Mail(
            Email(fromm),
            To(to),
            subject,
            Content("text/html", body)
        )
        mail.asm = Asm(GroupId(self.unsubscribe_group), GroupsToDisplay([self.unsubscribe_group]))
        response = sg_client.send(mail)
        logging.info(f"Sending a '{subject}' mail using SendGrid")
        logging.debug(f"Mail to {to} response\nStatus code: {response.status_code}\n{response.body}")
        return response
