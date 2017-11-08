import tornado.ioloop
import tornado.web
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from bson.code import Code
import re
import bson

# Configuracion de las caracteristicas de los servicios
class BaseHandler( tornado.web.RequestHandler ):
    def set_default_headers( self ):
        self.set_header( 'Access-Control-Allow-Origin', '*' )
        self.set_header( 'Access-Control-Allow-Headers', 'origin, x-requested-with, content-type' )
        self.set_header( 'Access-Control-Allow-Methods', 'POST, GET, PUT, DELETE, OPTIONS' )

# Deficion de la aplicacion REST
class Application( tornado.web.Application ):
    def __init__( self ):
        handlers = [
			(r'/', BaseHandler ),
            (r"/followers/(.*)", getFollowers),
            (r"/followersAll", getFollowersAll),
            (r"/general", getInfoGeneral),
            (r"/generalDos", getInfoGeneralDos),
            (r"/geo", getGeoSentiment),
            (r"/getLastTweetsByAccount/(.*)", getLastTweetsByAccount),
            (r"/getLastReplayByTweetID/(.*)", getLastReplayByTweetID),
            (r"/getLastSentimentReplayByTweetID/(.*)", getLastSentimentReplayByTweetID),
            (r"/topics", getTopics),
            (r"/images/(.*)", tornado.web.StaticFileHandler, {'path': "./images"}),
            (r"/cloud", doCloud),
            (r"/getMostFrequentWordsByUser", getMostFrequentWordsByUser),
            (r"/getFrequencyByTopic", getFrequencyByTopic),
            (r"/getFrequencyByTopicByUsername/(.*)", getFrequencyByTopicByUsername),
            (r"/getFrequencyByTopicUsedByUser/(.*)", getFrequencyByTopicUsedByUser)
		]
        tornado.web.Application.__init__( self, handlers )


class getInfoGeneral(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        self.write(dumps(mine.aggregate([{"$group": {"_id": "$sentiment", "value": {"$sum": 1}}},
            {"$project": {
                    "name": "$_id",
                    "value": 1,
                    "_id":0
            }}])))

class getInfoGeneralDos(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        json = []
        response_json = {}
        response_json["name"] = "Tweets"
        series = mine.aggregate([{'$group':{'_id':{'year':{'$year':'$tweet_date'},'month':{'$month':'$tweet_date'},'day':{'$dayOfMonth':'$tweet_date'}},'value':{'$sum':1},'name':{'$first':"$tweet_date"}}},{'$project':{'name':{'$dateToString':{'format':"%Y-%m-%d",'date':"$name"}},'value':1,'_id': 0}},
        {   '$sort':
            {
                'name': 1
            }
        }])
        response_json["series"] = series

        json.append(response_json)
        self.write(dumps(json))

class getGeoSentiment(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        self.write(dumps(mine.find({"tweet_location_lati" : {'$ne' : None}, }, {'screen_name':1, 'entities_mentions':1, 'entities_hashtags':1, 'text':1,'sentiment':1,'tweet_location_lati': 1,'tweet_location_long': 1, '_id':0})))

class getFollowers(BaseHandler):
    def get(self, name):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['users']

        json = []
        response_json = {}
        response_json["name"] = "Followers"

        series = mine.aggregate(
            [
                {'$match': {'screen_name': name}},
                {'$project':
                    {
                        '_id': 0, 'name': "$downloaded_date", 'value': "$followers_number"}
                }
            ])

        response_json["series"] = series

        json.append(response_json)
        self.write(dumps(json))

class getFollowersAll(BaseHandler):
    def get(self):
                personajes =['CGurisattiNTN24','DanielSamperO','ELTIEMPO','elespectador','NoticiasCaracol','NoticiasRCN','CaracolRadio','BluRadioCo','JuanManSantos','ClaudiaLopez','German_Vargas','AlvaroUribeVel','AndresPastrana_','TimoFARC','OIZuluaga','A_OrdonezM','JSantrich_FARC','IvanDuque','mluciaramirez','petrogustavo','DeLaCalleHum','FARC_EPaz'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['users']

                json = []


                for person in personajes:
                    response_json = {}
                    response_json["name"] = "Seguidores de " + person

                    series = mine.aggregate(
                    [
                        {'$match': {'screen_name': person}},
                        {'$project':
                            {
                                '_id': 0, 'name': "$downloaded_date", 'value': "$followers_number"}
                        }
                    ])

                    response_json["series"] = series

                    json.append(response_json)

                self.write(dumps(json))

class getLastTweetsByAccount(BaseHandler):
    def get(self, account):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        self.write(dumps(mine.aggregate([{ '$match': {'screen_name': account } }, {'$lookup': { 'from': 'tweets', 'localField': '_id', 'foreignField': 'in_reply_to_status_id', 'as': 'grp' }},{ '$project': { '_id': 1, 'text': 1 }},{'$sort': { 'tweet_date': -1 }},{ '$limit': 20 }])))

class getLastReplayByTweetID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        self.write(dumps(mine.find({ 'in_reply_to_status_id': long(float(str(id))) }, { '_id': 0, 'screen_name': 1, 'text': 1, 'sentiment': 1 })))

class getLastSentimentReplayByTweetID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        self.write(dumps(mine.aggregate([{'$match': { 'in_reply_to_status_id': long(float(str(id))) } }, { '$group': { '_id': '$sentiment', 'count': { '$sum': 1 }}},{ '$project' : { '_id': 0, 'name': '$_id', 'value': '$count' } }])))

#EN PRODUCCION
class getTopics(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['trends']
        self.write(dumps(mine.find().limit(2)))




class doCloud(BaseHandler):
    def get(self):


        text = "y forcing the closing of the figure window in my giant loop, so I do"

        wordcloud = WordCloud(max_font_size=200).generate(text)
        fig = plt.figure()
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        fig.savefig('images/foo.png')
        self.write(dumps('OK'))

class getMostFrequentWordsByUser(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        map = Code( "function() {"  
                    "var text = this.text;"
                    "if (text) { "
                    "text = text.toLowerCase().split(' ');"
                    "for (var i = text.length - 1; i >= 0; i--) {"
                    "if (text[i]) {"
                    "emit(text[i], 1);"
                    "}}}}")

        reduce = Code("function( key, values ) {"    
                      "var count = 0; "  
                      "values.forEach(function(v) {"          
                      "count +=v;"    
                      "});"
                      "return count;}")


        result = mine.map_reduce(map, reduce, "myresults", query={ 'screen_name': "elespectador" })
        json_result = []
        for doc in result.find():
            doc['name'] = doc['_id']
            json_result.append(doc)




        self.write(dumps(json_result))

class getFrequencyByTopic(BaseHandler):
    def get(self):
            temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada', 'justicia'];

            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['tweets']

            json = []

            for tema in temas:
                regx = re.compile(tema, re.IGNORECASE)
                val = mine.find({"text" : regx}).count()
                response_json = {}
                response_json["name"] = tema
                response_json["value"] = val

                json.append(response_json)

            self.write(dumps(json))

class getFrequencyByTopicByUsername(BaseHandler):
        def get(self, name):
            temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                     'justicia'];

            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['tweets']

            json = []

            for tema in temas:
                regx = re.compile(tema, re.IGNORECASE)
                val = mine.find({"$and":[ {"text" :regx}, {"entities_mentions":name} ]}).count()
                response_json = {}
                response_json["name"] = tema
                response_json["value"] = val

                json.append(response_json)

            self.write(dumps(json))

class getFrequencyByTopicUsedByUser(BaseHandler):
            def get(self, name):
                temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                         'justicia'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['tweets']

                json = []
                otrosJ = {};


                contador = 0;

                for tema in temas:
                    response_json = {}
                    regx = re.compile(tema, re.IGNORECASE)
                    val = mine.find({"$and": [{"text": regx}, {"screen_name": name}]}).count()
                    contador += val


                    if val > 0:
                        response_json["name"] = tema
                        response_json["value"] = val

                        json.append(response_json)

                total = mine.find({"screen_name": name}).count()
                otros = total - contador
                otrosJ["name"] = 'Otros temas'
                otrosJ["value"] = otros

                json.append(otrosJ)

                self.write(dumps(json))


#EN PRODUCCION
class getFrequencyByTopicByUsernameGetHashtags(BaseHandler):
            def get(self, name):
                temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                         'justicia'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['tweets']

                for tema in temas:
                    regx = re.compile(tema, re.IGNORECASE)
                    val = mine.find({"$and": [{"text": regx}, {"entities_mentions": name}]}).count()
                    response_json = {}
                    response_json["name"] = tema
                    response_json["value"] = val

                    #json.append(response_json)

                self.write(dumps(mine.find({"$and": [{"text": regx}, {"entities_mentions": name}]})))



#Metodo main
if __name__ == "__main__":
    app = Application()
    app.listen(8082)
    tornado.ioloop.IOLoop.current().start()