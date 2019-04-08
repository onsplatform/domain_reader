import falcon

import msgpack


class DomainResource:

    def on_get(self, req, resp, solution, app, map, filter, query):

        if solution == '' or app == '' or map == '':
            resp.status = falcon.HTTP_400
            return

        resp.data = msgpack.packb(doc, use_bin_type=True)
        resp.content_type = falcon.MEDIA_MSGPACK
        resp.status = falcon.HTTP_200
