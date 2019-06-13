import json

import falcon


class APIResponse(falcon.Response):
    def bad_request(self):
        self.status = falcon.HTTP_400

    def internal_error(self):
        self.status = falcon.HTTP_500

    def json(self, data, status_code=falcon.HTTP_200):
        if not data:
            return self.not_found()

        self.status = status_code
        self.body = json.dumps(data)

    def accepted(self, status_code=falcon.HTTP_200):
        self.status = status_code

    def not_found(self):
        self.status = falcon.HTTP_404
