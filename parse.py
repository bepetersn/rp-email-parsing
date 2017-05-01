#!/usr/bin/env python3

import contextlib
import functools
import tarfile
import email.header
import csv
import sys
import os
import re

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = 'output.csv'
DATA_FIELDNAMES = ('date', 'from', 'subject')
DEFAULT_CHARSET = 'utf-8'


class MessageArchiveUnpacker(object):

    def get_message_paths(self, path):

        """
        Expects: A path to a message archive (tar.gz format).
        Yields:  A tuple of each message path & the data
                 extracted from it, including the fields
                 found in DATA_FIELDNAMES.


        """

        archive = tarfile.open(name=path, mode='r:gz')
        for tar_info in archive.getmembers():
            if tar_info.isfile():
                yield tar_info.name


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
                        if re.match(r'\s+', lines[i+1]):
                            # TODO: handle this later.
                            pass
                        
                        header_body = \
                            decode_email_header(match.group(2))
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
            doublequote=False,
            delimiter='|',
            escapechar='\\'
        )


if __name__ == '__main__':

    unpacker = MessageArchiveUnpacker()
    parser = EmailParser()
    with get_csv_writer(OUTPUT_CSV, DATA_FIELDNAMES) as writer:

        writer.writeheader()
        for message_path in unpacker.get_message_paths(sys.argv[1]):
            with open(message_path, 'r') as msg:
                data = parser.parse_message(msg.read())       
                writer.writerow(data)

    print(os.path.join(CURRENT_DIR, OUTPUT_CSV))

