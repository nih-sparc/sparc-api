import logging
from string import Template
import boto3
from mailersend import emails
from app.config import Config
from static.sparc_logo_base64 import sparc_logo_base64

subject = "Message from SPARC Portal"

ses_client = boto3.client(
    "ses",
    aws_access_key_id=Config.SPARC_PORTAL_AWS_KEY,
    aws_secret_access_key=Config.SPARC_PORTAL_AWS_SECRET,
    region_name=Config.AWS_REGION,
)

mailer = emails.NewEmail(Config.MAILERSEND_API_KEY)

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

service_form_submission_request_confirmation_email = Template(f'''\
<html>
  <body style="font-family: Arial, sans-serif; line-height: 1.6;">
    <p>Hi $name,</p>
    <p>Thank you for your submission!</p>
    <p>We've successfully received your form and appreciate you taking the time to provide this information. The information you submitted is included below for your records.</p>
    <p>The SPARC Data and Resource Center (DRC) has a depth of expertise in developing and supporting digital resources that can be shared, cited, visualized, computed, and used for virtual experimentation. Your interest in SPARC supports FAIR data principles—making research data Findable, Accessible, Interoperable, and Reusable. We truly appreciate your commitment to contributing to the broader scientific community and supporting efforts that will benefit researchers for years to come.</p>
    <p>Our team will review your submission and make our best effort to get back to you within the next 5 business days. If you have any questions in the meantime, please don't hesitate to contact us at <a href="mailto:services@sparc.science">services@sparc.science</a>.</p>
    <p>Thank you again for your dedication to advancing scientific progress.</p>
    <p>Best regards,<br/>
    SPARC Data and Resource Center</p>
    <p>
      <img src="{sparc_logo_base64}" alt="SPARC Logo" style="max-width: 200px; height: auto; margin-bottom: 20px;"/><br/>
      <a href="https://sparc.science">https://sparc.science</a><br/>
      NIH-approved, HEAL-compliant repository<br/>
      Registered with re3data.org
    </p>
    <p>
      Your submission:
      $message
    </p>
  </body>
</html>
''')

creation_request_confirmation_email = Template(f'''\
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
      <img src="{sparc_logo_base64}" alt="SPARC Logo" style="max-width: 200px; height: auto; margin-bottom: 20px;"/><br/>
      <a href="https://sparc.science">https://sparc.science</a><br/>
      NIH-approved, HEAL-compliant repository<br/>
      Registered with re3data.org
    </p>
    <p>
      Your submission:
      $message
    </p>
  </body>
</html>
''')

anbc_form_creation_request_confirmation_email = Template(f'''\
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
      <img src="{sparc_logo_base64}" alt="SPARC Logo" style="max-width: 200px; height: auto; margin-bottom: 20px;"/><br/>
      <a href="https://sparc.science">https://sparc.science</a><br/>
      NIH-approved, HEAL-compliant repository<br/>
      Registered with re3data.org
    </p>
    <p>
      Your submission:
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
        self.from_name = "SPARC Portal"

    def send_email(self, name, email_address, message):
        body = f"{name}\n{email_address}\n{message}"
        ses_client.send_email(
            Source=self.ses_sender,
            Destination={"ToAddresses": [self.ses_sender]},
            Message={
                "Subject": {"Charset": self.charset, "Data": self.default_subject},
                "Body": {"Text": {"Charset": self.charset, "Data": body}},
            },
            SourceArn=self.ses_arn,
        )

    def mailersend_email(self, from_email, to, subject, body, reply_to_email=None, reply_to_name=None):
        data = {
            "from": {"email": from_email, "name": self.from_name},
            "to": [{"email": to}],
            "subject": subject,
            "html": body,
        }

        if reply_to_email:
            data["reply_to"] = {
                "email": reply_to_email,
                "name": reply_to_name or reply_to_email
            }

        response = mailer.send(data)
        if not str(response).startswith("2"):
            raise Exception(f"Email failed to send.")
        logging.info(f"Sending a '{subject}' mail using MailerSend")
        logging.debug(f"Mail to {to} response: {response}")
        return response

    def mailersend_email_with_attachment(self, from_email, to, subject, body, encoded_file, file_name, file_type):
        attachment = {
            "content": encoded_file,
            "filename": file_name,
            "content_type": file_type,
            "disposition": "attachment"
        }

        data = {
            "from": {"email": from_email, "name": self.from_name},
            "to": [{"email": to}],
            "subject": subject,
            "html": body,
            "attachments": [attachment],
        }

        response = mailer.send(data)
        if not str(response).startswith("2"):
            logging.error(f"MailerSend error: {response.status_code} {response.text}")
            raise Exception(f"Email failed to send.")
        logging.info(f"Sending a '{subject}' mail with attachment using MailerSend")
        logging.debug(f"Mail to {to} response: {response}")
        return response
