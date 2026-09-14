"""Run a read probe with an explicit AWS CLI profile, without printing credentials.

This short-lived development bridge does not implement credential renewal.
Usage: python3 with_profile.py --profile fluree-dev -- <command> <args>...
"""

import argparse
import json
import os
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("provide a command after --")
    result = subprocess.run([
        "aws", "configure", "export-credentials", "--profile", args.profile,
        "--format", "process",
    ], capture_output=True, text=True, check=False)
    if result.returncode:
        # Never include credential-process stdout in an error or a log.
        raise SystemExit("AWS CLI could not resolve the selected profile")
    credentials = json.loads(result.stdout)
    env = os.environ.copy()
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        env.pop(name, None)
    env.update({
        "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
        "AWS_REGION": args.region,
        "AWS_DEFAULT_REGION": args.region,
        "AWS_EC2_METADATA_DISABLED": "true",
    })
    if credentials.get("SessionToken"):
        env["AWS_SESSION_TOKEN"] = credentials["SessionToken"]
    raise SystemExit(subprocess.run(command, env=env, check=False).returncode)


if __name__ == "__main__":
    main()
