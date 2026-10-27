"""Serve a completed local snapshot; Ctrl+C stops the server."""
import argparse
import uvicorn
from web.app import create_app
from web.snapshot import PROJECT, safe_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--replay-run-id')
    args = parser.parse_args()
    safe_id(args.snapshot_id)
    if not 1 <= args.port <= 65535:
        parser.error('Port must be between 1 and 65535.')
    streaming_path=None
    if args.replay_run_id:
        safe_id(args.replay_run_id)
        streaming_path=PROJECT/'artifacts/streaming'/args.replay_run_id/'latest.json'
    uvicorn.run(create_app(PROJECT/'artifacts/web'/args.snapshot_id,args.replay_run_id,streaming_path), host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
