# -*- coding: utf-8 -*-

import logging
import subprocess
import socket
import os.path

class SendmailHandler(logging.Handler):
   sendmail = ['/usr/sbin/sendmail', '-i', '-t' ]

   def __init__(self, recipient, name='nobody', level=logging.NOTSET):
      logging.Handler.__init__(self, level=level)
      self.hostname = socket.gethostname()
      self.name = name
      self.recipient = recipient

   def subject(self, record):
      if record.exc_info:
         exc_type, exc_instance, exc_tb = record.exc_info
         while exc_tb.tb_next:
            exc_tb = exc_tb.tb_next
         frame = exc_tb.tb_frame
         return '{hostname} {levelname}: {exc_type} {exc_instance!s} at {func} ({file}:{line})'.format(
            hostname=self.hostname,
            levelname=record.levelname,
            exc_type=exc_type.__name__,
            exc_instance=exc_instance,
            func=frame.f_code.co_name,
            file=os.path.basename(frame.f_code.co_filename),
            line=exc_tb.tb_lineno,
            )
      else:
         return '{hostname}: {levelname} {message}'.format(
            hostname=self.hostname,
            levelname=record.levelname,
            message=(record.getMessage().split('\n', 1)[0]),
            )

   def emit(self, record):
      message = '''Subject: {subject}
From: {sender}@{hostname}
To: {recipient}

{message}
'''.format(
      subject=self.subject(record),
      sender=self.name,
      hostname=self.hostname,
      recipient=self.recipient,
      message=self.format(record),
      )
      sendmail = subprocess.Popen(self.sendmail, stdin=subprocess.PIPE)
      sendmail.communicate(message)
