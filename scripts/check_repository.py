"""Validate source/evidence JSON and keep generated assets outside Git. No services required."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

PROJECT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--include-untracked',action='store_true',help='Also inspect files being prepared for a commit')
    args=parser.parse_args()
    command=['git','ls-files','-z','--cached']
    if args.include_untracked:
        command+=['--others','--exclude-standard']
    paths=sorted(set(subprocess.check_output(command,cwd=PROJECT).split(b'\0'))-{b''})
    if not paths:
        raise ValueError('No repository files found.')
    ignored=subprocess.run(['git','check-ignore','--no-index','--stdin','-z'],cwd=PROJECT,
        input=b'\0'.join(paths)+b'\0',stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    if ignored.returncode not in (0,1):
        raise RuntimeError('Cannot validate ignore rules: '+ignored.stderr.decode(errors='replace'))
    problems=['Tracked file matches .gitignore: '+name.decode() for name in ignored.stdout.split(b'\0') if name]
    json_count=0
    for raw in paths:
        relative=raw.decode('utf-8')
        path=PROJECT/relative
        if path.stat().st_size>10*1024*1024:
            problems.append('File exceeds 10 MiB source/evidence limit: '+relative)
        if path.suffix.lower()=='.json':
            try:
                json.loads(path.read_bytes())
                json_count+=1
            except (ValueError,UnicodeError) as error:
                problems.append(f'Invalid JSON {relative}: {error}')
    if problems:
        raise ValueError('\n'.join(problems))
    print(f'PASS: {len(paths)} files, {json_count} JSON documents, no ignored/generated or oversized tracked files.')


if __name__=='__main__':
    try:
        main()
    except (OSError,ValueError,RuntimeError,subprocess.CalledProcessError) as error:
        print(str(error),file=sys.stderr)
        sys.exit(1)
