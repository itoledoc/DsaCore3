import WtoAlgorithm3 as Wto
import cherrypy


class StringGenerator(object):

    def __init__(self, datas):

        self.datas = datas

    @cherrypy.expose
    def index(self):
        return "Hello world!"

    @cherrypy.expose
    def generate(self, array="TWELVE-M"):
        return self.datas.schedblocks.query('array == @array').to_json()

if __name__ == '__main__':

    cherrypy.config.update({'server.socket_host': '10.200.113.71'})
    datas = Wto.WtoAlgorithm3()
    cherrypy.quickstart(StringGenerator(datas))