import falcon

import msgpack


class DomainResource(object):

    def on_get(self, req, resp, project, solution, map, filter):
        doc = {
            'usinas': [
                {
                    'name': 'ITAIPU'
                }
            ]
        }

        resp.data = msgpack.packb(doc, use_bin_type=True)
        resp.content_type = falcon.MEDIA_MSGPACK
        resp.status = falcon.HTTP_200
