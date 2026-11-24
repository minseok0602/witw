"""Portable source audit: normalize CRLF only; preserve every other byte."""
import argparse
import hashlib
import json
from pathlib import Path


def source_digest(path):
    return hashlib.sha256(Path(path).read_bytes().replace(b'\r\n',b'\n')).hexdigest()


def verify(repo,audit):
    expected=json.loads(Path(audit).read_text(encoding='utf-8-sig'))
    for name,value in expected.items():
        if source_digest(Path(repo)/name)!=value:raise ValueError('Source content changed since audit: '+name)
    return len(expected)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--audit',type=Path,required=True)
    a=p.parse_args();print('Audited source files:',verify(a.repo,a.audit))
