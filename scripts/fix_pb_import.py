#!/usr/bin/env python

from argparse import ArgumentParser, FileType

parser = ArgumentParser(description='fix gRPC import')
parser.add_argument('file', help='file to fix', type=FileType('rt'))
args = parser.parse_args()

old_import = 'import frames_pb2 as frames__pb2'
new_import = 'from . import frames_pb2 as frames__pb2'

lines = []
for line in args.file:
    if line.startswith(old_import):
        lines.append(new_import + '\n')
    else:
        lines.append(line)
args.file.close()

with open(args.file.name, 'wt') as out:
    out.write(''.join(lines))
