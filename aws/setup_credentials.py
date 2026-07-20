#!/usr/bin/env python3
# aws/setup_credentials.py
"""
AWS Credentials Setup
=====================
Configures programmatic access keys for boto3 so the platform can connect
to AWS account 021891603670 (MSiddique).

Your aws/.env_aws has the CONSOLE login (web browser only).
Boto3 needs PROGRAMMATIC ACCESS KEYS — a separate pair of credentials.

─── How to get your Access Keys ────────────────────────────────────────────

1. Open:  https://021891603670.signin.aws.amazon.com/console
2. Sign in:  Username = MSiddique10x
             Password = (from aws/.env_aws)
3. Top-right corner → click your username → "Security credentials"
4. Scroll to "Access keys" section → click "Create access key"
5. Select use case: "Command Line Interface (CLI)" → tick confirm → Next
6. Description tag: "spark-project-boto3" → click "Create access key"
7. COPY BOTH VALUES NOW (the secret is shown only once)
8. Run this script and paste them when prompted:

       python aws/setup_credentials.py

────────────────────────────────────────────────────────────────────────────

This script writes credentials to:
  ~/.aws/credentials   ← boto3 reads this automatically
  ~/.aws/config        ← sets default region to us-east-1
  .env                 ← project fallback (AWS_ACCESS_KEY_ID etc.)
"""
from __future__ import annotations

import configparser
import getpass
import os
import sys
from pathlib import Path

# ── project root so we can update .env ───────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent


def _mask(s: str) -> str:
    if len(s) <= 8:
        return "*" * len(s)
    return s[:4] + "*" * (len(s) - 8) + s[-4:]


# ─────────────────────────────────────────────────────────────────────────────

def check_existing() -> bool:
    """Return True and print identity if credentials already work."""
    try:
        import boto3
        sts      = boto3.client("sts", region_name="us-east-1")
        identity = sts.get_caller_identity()
        print("\nCredentials already configured and working:")
        print(f"  Account : {identity['Account']}")
        print(f"  ARN     : {identity['Arn']}")
        return True
    except Exception:
        return False


def write_aws_dir(access_key: str, secret_key: str, region: str) -> None:
    """Write ~/.aws/credentials and ~/.aws/config."""
    aws_dir = Path.home() / ".aws"
    aws_dir.mkdir(mode=0o700, exist_ok=True)

    # credentials file
    creds = configparser.ConfigParser()
    creds_path = aws_dir / "credentials"
    if creds_path.exists():
        creds.read(creds_path)
    if "default" not in creds:
        creds["default"] = {}
    creds["default"]["aws_access_key_id"]     = access_key
    creds["default"]["aws_secret_access_key"] = secret_key
    with open(creds_path, "w") as f:
        creds.write(f)
    creds_path.chmod(0o600)
    print(f"  Written: {creds_path}")

    # config file
    cfg = configparser.ConfigParser()
    cfg_path = aws_dir / "config"
    if cfg_path.exists():
        cfg.read(cfg_path)
    if "default" not in cfg:
        cfg["default"] = {}
    cfg["default"]["region"] = region
    cfg["default"]["output"] = "json"
    with open(cfg_path, "w") as f:
        cfg.write(f)
    print(f"  Written: {cfg_path}")


def update_dot_env(access_key: str, secret_key: str, region: str) -> None:
    """Add / overwrite AWS_* lines in the project .env."""
    env_path = _ROOT / ".env"
    text     = env_path.read_text(encoding="utf-8") if env_path.exists() else ""

    replacements = {
        "AWS_ACCESS_KEY_ID":     access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION":            region,
    }

    lines = text.splitlines()
    for key, val in replacements.items():
        found = False
        for i, line in enumerate(lines):
            stripped = line.lstrip("# ").split("=", 1)
            if stripped[0].strip() == key:
                lines[i] = f"{key}={val}"
                found = True
                break
        if not found:
            lines.append(f"{key}={val}")

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Updated: {env_path}")


def verify(access_key: str, secret_key: str, region: str) -> bool:
    """Set env vars and call STS to confirm the keys work."""
    os.environ["AWS_ACCESS_KEY_ID"]     = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_DEFAULT_REGION"]    = region
    try:
        import boto3
        # Reload session with new env vars
        import importlib, botocore.session
        importlib.reload(botocore.session)
        sts      = boto3.client(
            "sts",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        identity = sts.get_caller_identity()
        print(f"\n  SUCCESS - Connected to AWS!")
        print(f"  Account : {identity['Account']}")
        print(f"  ARN     : {identity['Arn']}")
        return True
    except Exception as exc:
        print(f"\n  FAILED  - {exc}")
        print("  Double-check you copied both keys correctly.")
        return False


# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print(__doc__)

    # ── already works? ────────────────────────────────────────────────────────
    if check_existing():
        ans = input("\nAlready working. Re-configure? [y/N]: ").strip().lower()
        if ans != "y":
            print("\nNothing changed. Run the platform with:")
            print("  python -m aws.aws_main --action test")
            return

    # ── collect keys ──────────────────────────────────────────────────────────
    print("\nPaste the keys from the IAM console:\n")
    access_key = input("  AWS Access Key ID     : ").strip()
    secret_key = getpass.getpass("  AWS Secret Access Key : ").strip()
    region     = input("  Default region [us-east-1]: ").strip() or "us-east-1"

    if not access_key or not secret_key:
        print("ERROR: Both keys are required.")
        sys.exit(1)

    print(f"\n  Key ID : {_mask(access_key)}")
    print(f"  Secret : {_mask(secret_key)}")
    print(f"  Region : {region}")

    confirm = input("\n  Save and verify? [Y/n]: ").strip().lower()
    if confirm == "n":
        print("Cancelled.")
        return

    # ── write ─────────────────────────────────────────────────────────────────
    print("\nWriting credentials...")
    write_aws_dir(access_key, secret_key, region)
    update_dot_env(access_key, secret_key, region)

    # ── verify ────────────────────────────────────────────────────────────────
    print("\nVerifying connection...")
    ok = verify(access_key, secret_key, region)

    if ok:
        print("\nAll done. Next steps:")
        print("  python -m aws.aws_main --action test     # confirm connection")
        print("  python -m aws.aws_main --action setup    # provision all AWS resources")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()