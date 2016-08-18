# encoding=utf-8
import pymongo
from items import InformationItem, TweetsItem, FollowsItem, FansItem
import psycopg2


class SinaPipleline(object):
    def process_item(self, item, spider):
        try:
            conn = psycopg2.connect(database="postgres", user="postgres", host="127.0.0.1", port="5432")
            cur = conn.cursor()
        except Exception, e:
            print "connect error", str(e)

        if isinstance(item, InformationItem):
            try:
                cur.execute("""insert into Information (_id,birthday,city,gender,marriage,nickname,num_fans,num_tweets,province,signature,url)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""", (item['_id'], item['Birthday'], item['City'],
                                                                   item['Gender'], item['Marriage'], item['NickName'],
                                                                   item['Num_Fans'], item['Num_Tweets'],
                                                                   item['Province'],
                                                                   item['Signature'], item['URL']))
                conn.commit()
                print "SUCC INSERT Information ONE ROWS !"
            except Exception, e:
                print e
                print "------------------------------------------------------------------------------"
                conn.rollback()
            finally:
                cur.close()
                conn.close()

        if isinstance(item, TweetsItem):
            try:
                cur.execute("""insert into Tweets (_id,co_oridinates,comment,content,id,likes,pubtime,tools,transfer)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s);""", (item['_id'], item['Co_oridinates'], item['Comment'],
                                                             item['Content'], item['ID'], item['Like'],
                                                             item['PubTime'], item['Tools'], item['Transfer']))
                conn.commit()
                print "SUCC INSERT Tweets ONE ROWS !"
            except Exception, e:
                print e
                print "------------------------------------------------------------------------------"
                conn.rollback()
            finally:
                cur.close()
                conn.close()

        if isinstance(item, FollowsItem):
            try:
                cur.execute("""insert into Follows (_id,follows_id)
                    values(%s,%s);""", (item['_id'], item['follows']))
                conn.commit()
                print "SUCC INSERT Follows ONE ROWS !"
            except Exception, e:
                print e
                print "------------------------------------------------------------------------------"
                conn.rollback()
            finally:
                cur.close()
                conn.close()

        if isinstance(item, FansItem):
            try:
                cur.execute("""insert into Fans (_id,fans_id)
                    values(%s,%s);""", (item['_id'], item['fans']))
                conn.commit()
                print "SUCC INSERT Fans ONE ROWS !"
            except Exception, e:
                print e
                print "------------------------------------------------------------------------------"
                conn.rollback()
            finally:
                cur.close()
                conn.close()

        return item
