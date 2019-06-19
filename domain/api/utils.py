import json
from uuid import UUID

import falcon


class UUIDEncoder(json.JSONEncoder):
    """
    Json encoder supporting UUID fields
    """

    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class APIResponse(falcon.Response):
    """
    Extended Falcon Response class.
    """

    def bad_request(self):
        """
        sets response status to HTTP 400 bad request.
        """
        self.status = falcon.HTTP_400

    def internal_error(self):
        """
        sets response status to HTTP 500 internal server error.
        """
        self.status = falcon.HTTP_500

    def json(self, data, status_code=falcon.HTTP_200):
        """
        embeds json data into response body.
        """
        if not data:
            return self.not_found()

        self.status = status_code
        self.body = json.dumps(data, cls=UUIDEncoder)

    def accepted(self, status_code=falcon.HTTP_200):
        """
        sets response status to HTTP 200 request accepted.
        """
        self.status = status_code

    def not_found(self):
        """
        sets response status to HTTP 404 not found.
        """
        self.status = falcon.HTTP_404


class APIRequest(falcon.Request):
    """
    Extended Falcon Request class.
    """
    def json(self):
        """
        request body as json
        """
        body = self.stream.read().decode('utf-8')
        return json.loads(body)

    @property
    def instance_id(self):
        """
        instance id from request.
        """
        return self.get_header('Instance-Id')

