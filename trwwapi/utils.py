from django.conf import settings

class DebugMessages():

    def __init__(self, messages=[], debug=settings.DEBUG):
        self.messages = messages
        self.show = debug

    def add(self, msg):
        if self.show:
            print(msg)
        self.messages.append(msg)

def _parse_request(request):
    """parse the django request object
    """

    # **parse** the arguments from the query string or body, depending on request method
    if request.method == 'GET':
        raw_args = request.query_params.dict()
    else:
        raw_args = request.data

    return raw_args