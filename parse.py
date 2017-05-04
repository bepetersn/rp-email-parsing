#!/usr/bin/env python3

import email.header
import contextlib
import subprocess
import functools
import tarfile
import quopri
import csv
import sys
import os
import re

import click


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CHARSET = 'utf-8'
SHOW_COMMAND = 'xdg-open {output_file}'

# These are fields which by default will be parsed for
# in the email headers of the message archive.
DATA_FIELDNAMES = ('date', 'from', 'subject', )
# This is the filename which by default will 
# contain the results of the parsing.
DEFAULT_OUTPUT_CSV = 'output.csv'


class MessageArchiveUnpacker(object):

    def get_messages(self, path):

        """
        Expects: A path to a message archive (tar.gz format).
        Returns: An iterator over some `io.BufferedReader`
                 instances, one for each message in the archive.
                 These are able to have their `read` method
                 called just once to get the message contents.

        """
        
        with tarfile.open(name=path, mode='r:gz') as archive:
            for tar_info in archive.getmembers():
                if tar_info.isfile():
                    yield archive.extractfile(tar_info)


class EmailParser(object):

    def parse_message(self, contents):

        """

        Expects: Text (bytes) contents of a raw email.
        Returns: Dict of data parsed from the contents,
                 for now just the fields listed in
                 the DATA_FIELDNAMES constant.

        """

        data = {}
        lines = contents.splitlines()
        for i, line in enumerate(lines):

            # If the line starts with whitespace,
            # we don't care about it--either we
            # already peeked ahead and captured
            # it as a continuation, or it's just an
            # empty line.
            if not re.match(r'\s+', line):

                # If the first part contains only
                # alphabetics or a hyphen, followed
                # immediately by a colon, then it
                # should be a new header field name.
                match = re.match(
                   r'([a-zA-Z\-]+): ?(.*)', line)
                if match is not None:

                    # In this case, we want to check
                    # the header fieldname to see if
                    # it's one of the ones we need
                    # to capture.
                    header_name = match.group(1).lower()
                    if header_name in DATA_FIELDNAMES:
                       
                        # before we collect the data,
                        # peek at the next line to make
                        # sure there wasn't a continuation
                        header_body = match.group(2)
                        peek_value = 0
                        while True:
                            peek_value += 1
                            next_line = lines[i+peek_value]
                            if re.match(r'\s+', next_line):
                                header_body += next_line
                            else:
                                break
                        
                        # Headers containing non-ascii 
                        # characters in their body have 
                        # to be decoded.
                        header_body = \
                            decode_email_header(header_body)
                        data.update({header_name: header_body})

                    else:
                        # No data we care about, so ignore.
                        continue

                else:
                    # If it doesn't start with whitespace,
                    # or a new header, then we've reached
                    # the end of the headers--and the data
                    # collection we're doing.
                    return data


def decode_email_header(header):
    """
    See RFC 1522. Non-ascii email header bodies
    can be encoded in two different ways, both 
    of which have to be handled: quoted-printable
    & base64 encoding.

    """
    header = decode_base64_encoded_email_header(header)
    x = quopri.decodestring(header).decode(DEFAULT_CHARSET)
    return x


def decode_base64_encoded_email_header(header):
    result = ''
    for str_or_bytes, encoding in email.header.decode_header(header):
        if hasattr(str_or_bytes, 'decode'):
            result += str(str_or_bytes, encoding or DEFAULT_CHARSET)
        else:
            result += str_or_bytes
    return result


@contextlib.contextmanager
def get_csv_writer(output_filepath, fieldnames):
    with open(output_filepath, 'wt', encoding='utf-8') as csvfile:
        yield csv.DictWriter(
            csvfile,
            fieldnames=fieldnames,
            extrasaction='ignore',
            quoting=csv.QUOTE_NONE,
            delimiter='|',
            escapechar='\\'
        )


@click.group()
def cli():
    pass


@cli.command()
@click.argument('archive-path')
@click.option('--output-file', '-o', default=DEFAULT_OUTPUT_CSV)
@click.option('--show-results/--dont-show-results', '-s/-z')
def run(archive_path, output_file, show_results):

    unpacker = MessageArchiveUnpacker()
    parser = EmailParser()
    with get_csv_writer(output_file, DATA_FIELDNAMES) as writer:

        writer.writeheader()
        for msg in unpacker.get_messages(archive_path):
            data = parser.parse_message(msg.read().decode(DEFAULT_CHARSET))
            writer.writerow(data)

    absolute_output_file = os.path.join(CURRENT_DIR, output_file)
    print('Result is at: {}'.format(absolute_output_file))

    if show_results:
        print('Opening...')
        subprocess.call(
            SHOW_COMMAND.format(output_file=output_file).split()
        )


if __name__ == '__main__':
    cli()
