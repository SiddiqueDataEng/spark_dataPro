#!/usr/bin/env python3
# aws/grant_permissions.py
"""
Grant Permissions to MSiddique10x
===================================
The IAM user MSiddique10x has no service policies attached yet.
This script connects as the ROOT account (or an admin user) to attach
the required managed policies, OR it prints the exact console steps
to do it manually in 2 minutes.

Two modes:
  1. Automatic  — if you have root/admin credentials available
  2. Manual     — prints step-by-step console instructions (default)

Manual steps (fastest — ~2 minutes):
  1. https://021891603670.signin.aws.amazon.com/console
  2. Sign in as MSiddique10x  (password: @Ibad#2018@)
     -- OR -- sign in as root account
  3. Go to:  IAM -> Users -> MSiddique10x -> Add permissions
  4. Choose: Attach policies directly
  5. Search and tick each policy below, then click "Next" -> "Add permissions"

Required policies (tick all 6):
  - AmazonS3FullAccess
  - AWSGlueConsoleFullAccess
  - AmazonAthenaFullAccess
  - AmazonRDSFullAccess
  - AWSLakeFormationDataAdmin
  - AWSStepFunctionsFullAccess

Optional (for DMS and CDK):
  - AmazonDMSFullAccess
  - AdministratorAccess    (gives everything — simplest for a dev account)

After attaching policies, run:
  python -m aws.aws_main --action test
  python -m aws.aws_main --action setup
"""
from __future__ import annotations

import sys
from pathlib import Path

# ─── Policies to attach to MSiddique10x ───────────────────────────────────────
REQUIRED_POLICIES = [
    ("AmazonS3FullAccess",              "arn:aws:iam::aws:policy/AmazonS3FullAccess"),
    ("AWSGlueConsoleFullAccess",        "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"),
    ("AmazonAthenaFullAccess",          "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"),
    ("AmazonRDSFullAccess",             "arn:aws:iam::aws:policy/AmazonRDSFullAccess"),
    ("AWSLakeFormationDataAdmin",       "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"),
    ("AWSStepFunctionsFullAccess",      "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"),
    ("AmazonDMSFullAccess",             "arn:aws:iam::aws:policy/AmazonDMSFullAccess"),
    ("IAMFullAccess",                   "arn:aws:iam::aws:policy/IAMFullAccess"),
]

# Single policy that covers everything (simplest for a personal dev account)
ADMIN_POLICY = ("AdministratorAccess", "arn:aws:iam::aws:policy/AdministratorAccess")

USERNAME = "MSiddique10x"


def print_manual_steps() -> None:
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  Grant AWS Permissions — Manual Console Steps (~2 minutes)          ║
╚══════════════════════════════════════════════════════════════════════╝

1. Open:  https://021891603670.signin.aws.amazon.com/console
2. Sign in:  MSiddique10x  /  @Ibad#2018@

3. Navigate to:
   Services → IAM → Users → MSiddique10x → "Add permissions" button

4. Choose: "Attach policies directly"

5. Search and tick this ONE policy (simplest — full access for dev):

       ✅  AdministratorAccess

   OR tick these 8 specific policies for least-privilege:
""")
    for name, arn in REQUIRED_POLICIES:
        print(f"       [OK]  {name}")

    print(f"""
6. Click "Next" → "Add permissions"

7. Done. Come back and run:
       python -m aws.aws_main --action test
       python -m aws.aws_main --action setup
""")


def auto_attach(admin_key_id: str, admin_secret: str, use_admin_policy: bool = True) -> None:
    """
    Attach policies programmatically using a second set of admin credentials.
    Call this if you have root or another admin user's keys available.

    Usage:
        python aws/grant_permissions.py --auto \\
            --key AKIA... --secret xxx [--least-privilege]
    """
    import boto3
    iam = boto3.client(
        "iam",
        aws_access_key_id=admin_key_id,
        aws_secret_access_key=admin_secret,
        region_name="us-east-1",
    )

    policies = [ADMIN_POLICY] if use_admin_policy else REQUIRED_POLICIES
    attached = 0

    for name, arn in policies:
        try:
            iam.attach_user_policy(UserName=USERNAME, PolicyArn=arn)
            print(f"  [OK]  Attached: {name}")
            attached += 1
        except iam.exceptions.EntityAlreadyExistsException:
            print(f"  -  Already attached: {name}")
        except Exception as exc:
            print(f"  [X]  Failed {name}: {exc}")

    print(f"\nAttached {attached} policies to {USERNAME}.")
    print("Run:  python -m aws.aws_main --action test")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Grant permissions to MSiddique10x IAM user"
    )
    parser.add_argument("--auto",           action="store_true",
                        help="Auto-attach using admin credentials")
    parser.add_argument("--key",            help="Admin AWS_ACCESS_KEY_ID")
    parser.add_argument("--secret",         help="Admin AWS_SECRET_ACCESS_KEY")
    parser.add_argument("--least-privilege",action="store_true",
                        help="Use 8 specific policies instead of AdministratorAccess")
    args = parser.parse_args()

    if args.auto:
        if not args.key or not args.secret:
            print("ERROR: --auto requires --key and --secret (admin credentials)")
            sys.exit(1)
        auto_attach(
            args.key, args.secret,
            use_admin_policy=not args.least_privilege,
        )
    else:
        print_manual_steps()


if __name__ == "__main__":
    main()