from twython import Twython
import pandas as pd

# Chile esta en UTC-4 en horario invierno,
# cambiar si estamos en horario de verano
hora_UTC = 4

# Twitter API credentials de cuenta,
# pedir en la página oficial de twitter para desarrollar APIs
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_KEY = ""
ACCESS_SECRET = ""

twitter = Twython(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET)
# puede ser maximo 200, tiene que ser menor a cantidad_tuits, ojala iguales
cantidad_tuits_por_query = 200

names = []  # lista de cuentas a ocupar

names.append('boTransporte')  # por ejemplo, agregamos la cuenta @boTransporte
cantidad_tuits = 3200  # máximo 3200, tope de las APIS de twitter gratuitas


def get_tweets(name, cantidad=None):
    if cantidad is None:
        cantidad = 3200  # en caso de no especificarse, asumimos 3200
    counter = 0
    new_tweets = twitter.get_user_timeline(screen_name=name,
                                           count=cantidad_tuits_por_query,
                                           include_rts=False,
                                           tweet_mode='extended')
    while new_tweets:
        if counter >= cantidad:
            break
        counter = counter + cantidad_tuits_por_query

        oldest = new_tweets[-1]['id'] - 1
        for tweet in new_tweets:
            yield tweet
        new_tweets = twitter.get_user_timeline(screen_name=name,
                                               count=cantidad_tuits_por_query,
                                               include_rts=False,
                                               max_id=oldest,
                                               tweet_mode='extended')


def bajar_procesar_tuits():

    nombre_de_columnas = ['id', 'autor', 'link', 'datetime', 'dia',
                          'texto_tweet', 'RT', 'FAV', 'user_mention1',
                          'user_mentions', 'n_user_mentions', 'hashtag1',
                          'hashtags', 'n_hashtags', 'geo', 'coordinates',
                          'place_name']
    df_proces = pd.DataFrame(index=range(cantidad_tuits * len(names)),
                             columns=nombre_de_columnas)
    i = 0

    for name in names:
        j = 0
        print(name)
        try:
            los_tweets = get_tweets(name, cantidad_tuits)
            for tweet in los_tweets:
                j = j + 1
                if j % 200 == 0:
                    print(j)
                if tweet['favorited'] is False and tweet['retweeted'] is False:
                    df_proces.loc[i, 'id'] = tweet['id_str']
                    df_proces.loc[i, 'autor'] = name
                    df_proces.loc[i, 'link'] = ('https://twitter.com/' + name +
                                                '/status/' + tweet['id_str'])
                    df_proces.loc[i, 'datetime'] = tweet['created_at']
                    df_proces.loc[i, 'texto_tweet'] = tweet['full_text']
                    df_proces.loc[i, 'RT'] = tweet['retweet_count']
                    df_proces.loc[i, 'FAV'] = tweet['favorite_count']
                    if len(tweet['entities']['user_mentions']) > 0:
                        df_proces.loc[i, 'user_mention1'] = (tweet['entities']
                                                             ['user_mentions']
                                                             [0]
                                                             ['screen_name'])
                        df_proces.loc[i, 'n_user_mentions'] = len(tweet['entities']['user_mentions'])
                        df_proces.loc[i, 'user_mentions'] = ''
                        for user_ment in tweet['entities']['user_mentions']:
                            df_proces.loc[i, 'user_mentions'] = (df_proces.loc[i, 'user_mentions'] +
                                                                user_ment['screen_name'] + ',')
                        df_proces.loc[i, 'user_mentions'] = df_proces.loc[i, 'user_mentions'][:-1]
                    else:
                        df_proces.loc[i, 'n_user_mentions'] = 0

                    if len(tweet['entities']['hashtags']) > 0:
                        df_proces.loc[i, 'hashtag1'] = tweet['entities']['hashtags'][0]['text']
                        df_proces.loc[i, 'n_hashtags'] = len(tweet['entities']['hashtags'])
                        df_proces.loc[i, 'hashtags'] = ''
                        for hasht in tweet['entities']['hashtags']:
                            df_proces.loc[i, 'hashtags'] = (df_proces.loc[i, 'hashtags'] +
                                                           hasht['text'] + ',')
                        df_proces.loc[i, 'hashtags'] = df_proces.loc[i, 'hashtags'][:-1]
                    else:
                        df_proces.loc[i, 'n_hashtags'] = 0

                    if tweet['geo']:
                        df_proces.loc[i, 'geo'] = tweet['geo']

                    if tweet['coordinates']:
                        df_proces.loc[i, 'coordinates'] = tweet['coordinates']

                    if tweet['place']:
                        df_proces.loc[i, 'place_name'] = tweet['place']['full_name']

                    i += 1
        except Exception as e:
            print("Muy probablemente no existe la cuenta " + name + " o te ha bloqueado")

    df_proces = df_proces.dropna(axis=0, how='all')
    df_proces['datetime'] = pd.to_datetime(df_proces['datetime'])
    df_proces['datetime'] = df_proces['datetime'].apply(lambda x: x - pd.DateOffset(hours=hora_UTC))
    df_proces['datetime'] = df_proces['datetime'].apply(lambda x: x.tz_localize(None))

    df_proces['dia'] = df_proces['datetime'].apply(lambda x: x.weekday())

    pd.options.display.max_colwidth = 300
    df_proces.to_excel('lavintweets.xlsx', index=False)


if __name__ == "__main__":

    print('MAIN: bajar_procesar_tuits')
    bajar_procesar_tuits()
