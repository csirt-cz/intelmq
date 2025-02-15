#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Initiate dialog ex like this:
# ssh -t $USER@proki.csirt.cz 'docker exec -it -u 1000 intelmq intelmq.bots.outputs.mail.output mail-output-cz cli
#   --tester [your-email] --ignore-older-than-days 4'
#
from __future__ import unicode_literals

import argparse
import csv
import datetime
import json
import os
import sys
import time
import zipfile
from base64 import b64decode
from collections import namedtuple, OrderedDict

import redis.exceptions
from envelope import Envelope

from intelmq.lib.bot import Bot
from intelmq.lib.cache import Cache

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

Mail = namedtuple('Mail', ["key", "to", "path", "count"])


class MailSendOutputBot(Bot):
    TMP_DIR = "/tmp/intelmq-mails/"
    mail_contents: str
    alternative_mail: dict
    timeout: list
    cache: Cache
    key: str

    def process(self):
        message = self.receive_message()

        if "source.abuse_contact" in message:
            field = message["source.abuse_contact"]
            self.logger.debug(f"{self.key}{field}")
            for mail in (field if isinstance(field, list) else [field]):
                self.cache.redis.rpush(f"{self.key}{mail}", message.to_json())

        self.acknowledge_message()

    def set_cache(self):
        self.cache = Cache(
            self.parameters.redis_cache_host,
            self.parameters.redis_cache_port,
            self.parameters.redis_cache_db,
            self.parameters.redis_cache_ttl
        )

    def init(self):
        self.set_cache()
        self.key = f"{self._Bot__bot_id}:"
        if "cli" in sys.argv:  # assure the launch is not handled by intelmqctl
            parser = argparse.ArgumentParser(prog=" ".join(sys.argv[0:1]))
            parser.add_argument('cli', help='initiate cli dialog')
            parser.add_argument('--tester', dest="testing_to", help='tester\'s e-mail')
            parser.add_argument('--ignore-older-than-days',
                                help='1..n skip all events with time.observation'
                                     ' older than 1..n day; 0 disabled (allow all)',
                                type=int)
            parser.add_argument("--gpg-key", help="fingerprint of gpg key to be used")
            parser.add_argument("--limit-results", type=int, help="Just send first N mails.")
            parser.add_argument("--send", help="Sends now, without dialog.", action='store_true')
            parser.parse_args(sys.argv[2:], namespace=self.parameters)

            if self.parameters.cli == "cli":
                self.cli_run()

    def cli_run(self):
        os.makedirs(self.TMP_DIR, exist_ok=True)
        with open(self.parameters.mail_template) as f:
            self.mail_contents = f.read()
        self.alternative_mail = {}
        if hasattr(self.parameters, "alternative_mails"):
            with open(self.parameters.alternative_mails, "r") as f:
                for row in csv.reader(f, delimiter=","):
                    self.alternative_mail[row[0]] = row[1]

        print("Preparing mail queue...")
        self.timeout = []
        mails = [m for m in self.prepare_mails() if m]

        print("")
        if self.parameters.limit_results:
            print(f"Results limited to {self.parameters.limit_results} by flag. ", end="")

        if self.timeout:
            print("Following address has timed out and will not be sent! :(")
            print(self.timeout)

        if not len(mails):
            print(" *** No mails in queue ***")
            sys.exit(0)
        else:
            print("Number of mails in the queue:", len(mails))

        while True:
            print("GPG active" if self.parameters.gpg_key else "No GPG")
            print("\nWhat you would like to do?\n"
                  f"* enter to send first mail to tester's address {self.parameters.testing_to}.\n"
                  "* any mail from above to be delivered to tester's address\n"
                  "* 'debug' to send all the e-mails to tester's address")
            if self.parameters.testing_to:
                print("* 's' for setting other tester's address")
            print("* 'all' for sending all the e-mails\n"
                  "* 'clear' for clearing the queue\n"
                  "* 'x' to cancel\n"
                  "? ", end="")

            if self.parameters.send:
                print(" ... Sending now!")
                i = "all"
            else:
                i = input()

            if i in ["x", "q"]:
                sys.exit(0)
            elif i == "all":
                count = 0
                for mail in mails:
                    if self.build_mail(mail, send=True):
                        count += 1
                        print(f"{mail.to} ", end="", flush=True)
                        try:
                            self.cache.redis.delete(mail.key)
                        except redis.exceptions.TimeoutError:
                            time.sleep(1)
                            try:
                                self.cache.redis.delete(mail.key)
                            except redis.exceptions.TimeoutError:
                                print(f"\nMail {mail.to} sent but could not be deleted from redis."
                                      f" When launched again, mail will be send again :(.")
                        if mail.path:
                            os.unlink(mail.path)
                print(f"\n{count}× mail sent.\n")
                sys.exit(0)
            elif i == "clear":
                for mail in mails:
                    self.cache.redis.delete(mail.key)
                print("Queue cleared.")
                sys.exit(0)
            elif i == "s":
                self.set_tester()
            elif i in ["", "y"]:
                self.send_mails_to_tester([mails[0]])
            elif i == "debug":
                self.send_mails_to_tester(mails)
            else:
                for mail in mails:
                    if mail.to == i:
                        self.send_mails_to_tester([mail])
                        break
                else:
                    print("Unknown option.")

    def set_tester(self, force=True):
        if not force and self.parameters.testing_to:
            return
        print("\nWhat e-mail should I use?")
        self.parameters.testing_to = input()

    def send_mails_to_tester(self, mails):
        """
            These mails are going to tester's address. Then prints out their count.
        :param mails: list
        """
        self.set_tester(False)
        count = sum([1 for mail in mails if self.build_mail(mail, send=True, override_to=self.parameters.testing_to)])
        print(f"{count}× mail sent to: {self.parameters.testing_to}\n")

    def prepare_mails(self):
        """ Generates Mail objects """
        allowed_fieldnames = ['time.source', 'source.ip', 'classification.taxonomy', 'classification.type',
                              'time.observation', 'source.geolocation.cc', 'source.asn', 'event_description.text',
                              'malware.name', 'feed.name', 'feed.url', 'raw']
        fieldnames_translation = {'time.source': 'time_detected', 'source.ip': 'ip', 'classification.taxonomy': 'class',
                                  'classification.type': 'type', 'time.observation': 'time_delivered',
                                  'source.geolocation.cc': 'country_code', 'source.asn': 'asn',
                                  'event_description.text': 'description', 'malware.name': 'malware',
                                  'feed.name': 'feed_name', 'feed.url': 'feed_url', 'raw': 'raw'}

        for mail_record in self.cache.redis.keys(f"{self.key}*")[slice(self.parameters.limit_results)]:
            lines = []
            self.logger.debug(mail_record)
            try:
                messages = self.cache.redis.lrange(mail_record, 0, -1)
            except redis.exceptions.TimeoutError:
                print(f"Trying again: {mail_record}... ", flush=True)
                for s in range(1, 4):
                    time.sleep(s)
                    try:
                        messages = self.cache.redis.lrange(mail_record, 0, -1)
                        print("... Success!", flush=True)
                        break
                    except redis.exceptions.TimeoutError:
                        print("... failed ...", flush=True)
                        continue
                else:
                    # XX will be visible both warning and print?
                    print(f"Warning: {mail_record} timeout, too big to read from redis", flush=True)
                    self.logger.warning(f"Warning: {mail_record} timeout, too big to read from redis")
                    self.timeout.append(mail_record)
                    continue

            lines.extend(json.loads(str(message, encoding="utf-8")) for message in messages)

            # prepare rows for csv attachment
            threshold = datetime.datetime.now() - datetime.timedelta(
                days=self.parameters.ignore_older_than_days) if getattr(self.parameters, 'ignore_older_than_days',
                                                                        False) else False
            fieldnames = set()
            rows_output = []
            for row in lines:
                if threshold and row["time.observation"][:19] < threshold.isoformat()[:19]:
                    continue
                fieldnames = fieldnames | set(row.keys())
                keys = set(allowed_fieldnames).intersection(row)
                ordered_keys = []
                for field in allowed_fieldnames:
                    if field in keys:
                        ordered_keys.append(field)
                try:
                    row["raw"] = b64decode(row["raw"]).decode("utf-8").strip().replace("\n", r"\n").replace("\r", r"\r")
                except (ValueError, KeyError):  # not all events have to contain the "raw" field
                    pass
                rows_output.append(OrderedDict({fieldnames_translation[k]: row[k] for k in ordered_keys}))

            # prepare headers for csv attachment
            ordered_fieldnames = []
            for field in allowed_fieldnames:
                ordered_fieldnames.append(fieldnames_translation[field])

            # write data to csv
            output = StringIO()
            dict_writer = csv.DictWriter(output, fieldnames=ordered_fieldnames)
            dict_writer.writerow(dict(zip(ordered_fieldnames, ordered_fieldnames)))
            dict_writer.writerows(rows_output)

            email_to = str(mail_record[len(self.key):], encoding="utf-8")
            count = len(rows_output)
            if not count:
                path = None
            else:
                filename = f'{time.strftime("%y%m%d")}_{count}_events'
                path = self.TMP_DIR + filename + '_' + email_to + '.zip'

                zf = zipfile.ZipFile(path, mode='w', compression=zipfile.ZIP_DEFLATED)
                # noinspection PyBroadException
                try:
                    zf.writestr(filename + ".csv", output.getvalue())
                except Exception:
                    self.logger.error(f"Cannot zip mail {mail_record}")
                    continue
                finally:
                    zf.close()

                if email_to in self.alternative_mail:
                    print(f"Alternative: instead of {email_to} we use {self.alternative_mail[email_to]}")
                    email_to = self.alternative_mail[email_to]

            mail = Mail(mail_record, email_to, path, count)
            self.build_mail(mail, send=False)
            if count:
                yield mail

    # def _hasTestingTo(self):
    #    return hasattr(self.parameters, 'testing_to') and self.parameters.testing_to != ""

    def build_mail(self, mail, send=False, override_to=None):
        """ creates a MIME message
        :param mail: Mail object
        :param send: True to send through SMTP, False for just printing the information
        :param override_to: Use this e-mail instead of the one specified in the Mail object
        :return: True if successfully sent.

        """
        if override_to:
            intended_to = mail.to
            email_to = override_to
        else:
            intended_to = None
            email_to = mail.to
        email_from = self.parameters.email_from
        text = self.mail_contents
        try:
            subject = time.strftime(self.parameters.subject)
        except ValueError:
            subject = self.parameters.subject
        if intended_to:
            subject += f" (intended for {intended_to})"
        else:
            subject += f" ({email_to})"

        if send is True:
            if not mail.count:
                return False
            return (Envelope(text)
                    .attach(path=mail.path, name=f'proki_{time.strftime("%Y%m%d")}.zip')
                    .from_(email_from).to(email_to)
                    .bcc([] if intended_to else getattr(self.parameters, 'bcc', []))
                    .subject(subject)
                    .gpg()
                    .smtp(self.parameters.smtp_server)
                    .signature(self.parameters.gpg_key, self.parameters.gpg_pass).send())
        else:
            print(f'To: {email_to}; Subject: {subject} ', end="")
            if not mail.count:
                print("Will not be send, all events skipped")
            else:
                print(f'Events: {mail.count}, Size: {os.path.getsize(mail.path)}')
            return None


BOT = MailSendOutputBot
