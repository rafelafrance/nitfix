"""Generate UUIDs for labels."""

import sys
import uuid


try:
    N = int(sys.argv[1])
except Exception:  # pylint: disable=broad-except
    print('Enter the number of UUIDs to generate.')
    sys.exit(1)

for i in range(N):
    print(uuid.uuid4())
