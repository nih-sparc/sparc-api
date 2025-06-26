import logging
from string import Template
import boto3
import sendgrid
from app.config import Config
from sendgrid.helpers.mail import Asm, Cc, Content, Email, Mail, To, Attachment, FileName, FileType, Disposition, \
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
<html>
  <body style="font-family: Arial, sans-serif; line-height: 1.6;">

    <p>Hi $name,</p>

    <p>Thank you for your submission!</p>

    <p>We've successfully received your form and appreciate you taking the time to provide this information. The information you submitted is included below for your records.</p>

    <p>By participating in this process, you're helping advance FAIR data principles—making research data Findable, Accessible, Interoperable, and Reusable. We truly appreciate your commitment to contributing to the broader scientific community and supporting efforts that will benefit researchers for years to come.</p>

    <p>Our team will review your submission and get back to you within the next 3 business days. If you have any questions in the meantime, please don't hesitate to contact us at <a href="mailto:services@sparc.science">services@sparc.science</a>.</p>

    <p>Thank you again for your dedication to advancing scientific progress.</p>

    <p>Best regards,<br/>
    SPARC Data and Resource Center</p>

    <p>
      <img src="https://sparc.science/logo-sparc-wave-primary.svg" alt="SPARC Logo" style="max-width: 200px; height: auto; margin-bottom: 20px;"/><br/>
      <a href="https://sparc.science">https://sparc.science</a><br/>
      NIH-approved, HEAL-compliant repository<br/>
      Registered with re3data.org
    </p>

    <p>
      Your submission:
      <br/>
      <br/>
      $message
    </p>
  </body>
</html>
''')

anbc_form_creation_request_confirmation_email = Template('''\
<html>
  <body style="font-family: Arial, sans-serif; line-height: 1.6;">

    <p>Hi $name,</p>

    <p>Thank you for your submission!</p>

    <p>We've successfully received your form and appreciate you taking the time to provide this information. The information you submitted is included below for your records.</p>

    <p>By participating in this process, you're helping advance FAIR data principles—making research data Findable, Accessible, Interoperable, and Reusable. Through our partnership between Autonomic Neuroscience: Basic and Clinical and <a href="https://sparc.science/">SPARC</a>, we're working together to create a more robust and accessible scientific ecosystem. We truly appreciate your commitment to contributing to the broader scientific community and supporting efforts that will benefit researchers for years to come.</p>

    <p>Our team will review your submission and get back to you within the next 3 business days. If you have any questions in the meantime, please don't hesitate to contact us at <a href="mailto:services@sparc.science">services@sparc.science</a>.</p>

    <p>Thank you again for your dedication to advancing scientific progress.</p>

    <p>Best regards,<br/>
    SPARC Data and Resource Center</p>

    <p>
      <img src="https://sparc.science/logo-sparc-wave-primary.svg" alt="SPARC Logo" style="max-width: 200px; height: auto; margin-bottom: 20px;"/><br/>
      <a href="https://sparc.science">https://sparc.science</a><br/>
      NIH-approved, HEAL-compliant repository<br/>
      Registered with re3data.org
    </p>

    <p>
      Your submission:
      <br/>
      <br/>
      $message
    </p>
  </body>
</html>
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

    def sendgrid_email(self, fromm, to, subject, body, cc=None):
        mail = Mail(
            Email(fromm),
            To(to),
            subject,
            Content("text/html", body)
        )
        if cc:
          if isinstance(cc, list):
              for cc_addr in cc:
                  mail.add_cc(Cc(cc_addr))
          else:
              mail.add_cc(Cc(cc))

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
