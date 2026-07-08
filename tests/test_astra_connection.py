# Smoke test: verifies Astra DB credentials and connectivity by querying the account table.
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))


import loader

if __name__ == '__main__':
    try:
        session = loader.get_session()
        row = session.execute("select release_version from system.local").one()
        print(f"Astra DB connection OK. {row[0]}")
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
