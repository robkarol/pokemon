"""Assign pre-accounts collection rows and binders to one account.

    docker exec pokemon python -m app.claim_legacy <authentik-username>
"""
import sys

from app.database import claim_legacy_data, init_db, legacy_summary


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1].strip():
        print(__doc__)
        return 2
    user = sys.argv[1].strip()
    init_db()
    print("unclaimed:", legacy_summary())
    print(f"claimed for {user!r}:", claim_legacy_data(user))
    return 0


if __name__ == "__main__":
    sys.exit(main())
