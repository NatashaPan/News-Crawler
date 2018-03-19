import requests
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
import re


def facebook_crawler():
    post_datas = []
    post_data_dicts = []
    # page_id = "221136024677086" # E火舞團為例
    fan_page_id = re.findall("https://www.facebook.com/(.+)/", input("請輸入欲爬取之粉專網址 = "))[0]
    token = input("請輸入您的 token = ")
    db_name = input("請輸入欲存入之 DB 名稱 : ")
    collecion_name = input("請輸入欲存入之 collection 名稱 : ")
    domain = "https://graph.facebook.com/v2.12/%s?fields=id,name,posts{created_time,id,message,likes.limit(0).summary(True),story}&access_token=%s" % (
    fan_page_id, token)
    res = requests.get(domain)
    fan_page_name = res.json()['name']
    print("粉絲專業名稱 : " + fan_page_name + "\n" + "資料爬取中，請稍後......")
    res_json = res.json()['posts']
    print(res_json)
    while True:
        for data in res_json['data']:
            # find comment first
            comment_json = requests.get(
                "https://graph.facebook.com/v2.12/%s?fields=comments{created_time,id,message,likes.limit(0).summary(True),from}&access_token=%s" % (
                data['id'], token)).json()
            comments = []
            comments_dicts = []
            while True:
                if "comments" in comment_json:
                    for comment in comment_json["comments"]["data"]:
                        comment_id = comment["id"]
                        comment_message = comment["message"]
                        comment_likes = comment["likes"]["summary"]["total_count"]
                        comment_created_time = datetime.strptime(comment["created_time"].split("+")[0],
                                                                 "%Y-%m-%dT%H:%M:%S")
                        comments.append([comment_id, comment_message, comment_likes, comment_created_time])
                        comments_dicts.append(
                            {"id": comment_id, "message": comment_message, "total likes": comment_likes,
                             "created time": comment_created_time})

                    if 'paging' in comment_json and 'next' in comment_json['paging']:
                        comment_json = requests.get(comment_json['paging']['next']).json()
                        # print(comment_json)
                    else:
                        break
                else:
                    break
            post_id = data['id']
            another_story = data['story'] if 'story' in data  else '影片上傳'
            # the video upload message of fan page need using "graph video API"
            # so the data may not contain both story and message
            post_message = data['message'] if 'message' in data else another_story
            post_likes = data["likes"]["summary"]["total_count"]
            post_story = data['story'] if 'story' in data and post_message != '影片上傳' else '純文字'
            post_created_time = datetime.strptime(data['created_time'].split("+")[0], '%Y-%m-%dT%H:%M:%S')
            post_datas.append(
                [fan_page_name, post_id, post_message, post_likes, post_story, post_created_time, comments])
            post_data_dicts.append(
                {"id": post_id, "message": post_message, "total likes": post_likes, "story": post_story,
                 "created time": post_created_time, "comments": comments_dicts})
        if 'next' in res_json['paging']:
            res_json = requests.get(res_json['paging']['next']).json()
        else:
            break

    print("資料爬取完畢")
    posts_dataframe = pd.DataFrame(post_datas, columns=['粉絲專頁', '發文ID', '發文內容', '發文讚數', '發文類型', '發文時間', '回文'])
    posts_dataframe.to_csv('D:/woodnata_note/{}_data.csv'.format(fan_page_name), encoding='utf-8')

    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client[db_name]
    collection = db[collecion_name]
    collection.insert_many(post_data_dicts)
    return posts_dataframe

def main():
    facebook_crawler()

if __name__ == "__main__":
    this_df = main()
    print(this_df)
