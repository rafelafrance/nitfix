import uuid
import base64


for _ in range(10):
    guid = uuid.uuid4()
    short = base64.b64encode(guid.bytes).decode('utf-8')
    print(guid, '\t', short)
