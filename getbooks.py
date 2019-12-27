# coding=utf8
import pymysql
from bs4 import BeautifulSoup
import time
import csv
import codecs
from urllib.request import urlopen
from urllib import request
import re
import random
import requests
import json
import urllib
# 打开数据库连接
def change(str):
    # utf8str = abstract.encode('utf8', "replace")  # 进行编码
    # abstract = utf8str.decode('utf8')  # 进行解码
    tmpstr=str.encode('utf8', "replace")
    str= tmpstr.decode('utf8')
    str=str.replace('"', '“')
    return '"'+str+'"'
#得到需要爬的bookurl基础以及所有的bookid
def getbookurl(cursor):#得到随机选的urllist
    sql = "select url from book where id%6=0 and isBundle=0"
    cursor.execute(sql)
    data = cursor.fetchall()
    # print(data)
    urlist = []
    for m in range(len(data)):
        # print(data[m][0])
        urlist.append(data[m][0])
    # print(urlist)
    sql2="select id from book"
    cursor.execute(sql2)
    bookdata=cursor.fetchall()
    # print("\n")
    # print(bookdata)
    booklist=[]
    for m in range(len(bookdata)):
        # print(data[m][0])
        booklist.append(bookdata[m][0])
    # print(booklist)
    return urlist,booklist#前者bookurl,后者bookid
#得到可能要爬的peopleurl,参数是一个bookurl
def getpeopleurl(bookurl,repe):# repe存已有的peopleurl
    #爬的时候需要在后面+ebook
    #https://read.douban.com/ebook/3650..../
    #得到ebook页面的
    # try:
    #     html = urlopen(bookurl,timeout=10).read().decode('utf-8')
    # except:
    #     print("Time out")
    #     return repe
    # soup = BeautifulSoup(html, features='lxml')  # 用lxml解析
    # span1 = soup.find_all("a", {"href": re.compile("/people/(\d{5,10})/+$")})
    #
    # if span1:
    #     for l in span1:
    #         # print(l.get('href'))  # tag a 属性 href
    #         # print(type(l.get("href")))#str类型
    #         tmp = l.get("href")
    #         numtmp = re.findall("\d+", tmp)[0]
    #         numtmp = int(numtmp)
    #         if numtmp not in repe:
    #             repe.append(numtmp)

    #得到ratings页面的
    url2 = bookurl+"ratings"
    # print(url2)
    try:
        url3=urlopen(url2,timeout=10).read().decode('utf-8')
    except:
        print("Time out")
        return repe
    soup2 = BeautifulSoup(url3, features='lxml')  # 用lxml解析
    span2 = soup2.find_all("a", {"href": re.compile("/people/(\d{5,10})/+$"), "class": "review-author url fn"})
    if span2:
        for l in span2:
            tmp = l.get('href')
            numtmp = re.findall("\d+", tmp)[0]
            numtmp = int(numtmp)
            if numtmp not in repe:
                repe.append(numtmp)
    return repe

#开始爬取peopleurl 并实现数据库持久化

def get_tx3cbg(str,url):
    url = 'https://read.douban.com/j/search_v2'
    headers = {
        'Content-Type': 'application/json',
        'Cookie': '自己写',
        'Referer': 'https://read.douban.com/search?q='+urllib.parse.quote(str, safe='/', encoding=None, errors=None),
        'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (Kbook, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        'Host': 'read.douban.com',
        'Origin': 'https: // read.douban.com'
    }
    payload={自己填}
    # print(urllib.parse.quote(str, safe='/', encoding=None, errors=None))
    book = requests.post(url, data=json.dumps(payload), headers=headers,timeout=(14))
    time.sleep(random.random() * 3)
    # print(book.text)

    book = json.loads(book.text)
    return book#得到json数据

def change2num(m):
    if(m=="★★★★★"):
        return 5.0
    elif(m=="★★★★☆"):
        return 4.0
    elif(m=="★★★☆☆"):
        return 3.0
    elif (m == "★★☆☆☆"):
        return 2.0
    elif (m == "☆☆☆☆☆"):
        return 0.0
    elif (m == "★☆☆☆☆"):
        return 1.0

def saverate(cursor,book_id):
    userurl = "https://read.douban.com"
    try:
        # html = urlopen("https://read.douban.com/people/66949295/ebook?start=10&sort=score", timeout=10).read().decode('utf-8')
        html = urlopen(userurl + "/ebook/%d/"%book_id, timeout=10).read().decode('utf-8')
    except:
        print("Time out")
        return
    soup = BeautifulSoup(html, features='lxml')  # 用lxml解析
    span = soup.find_all("h1", {"class": "article-title"})
    title=span[0].get_text()

    span2 = soup.find_all("span", {"class": "score"})
    span3 = soup.find_all("a", {"href": re.compile("/ebook/(\d{5,10})/ratings")})
    avscore=0.0
    scorenum=0
    if span2:
        if span2[0].get_text():
            avscore=float(span2[0].get_text()) / 2
    if span3:
        if span3[0].get_text():
            scorenum = re.sub("[\u4e00-\u9fa5]", "", span3[0].get_text())
    # print(span)
    # print(float(span2[0].get_text()) / 2)
    # num = re.sub("[\u4e00-\u9fa5]", "", span3[0].get_text())

    burl = "https://read.douban.com/search?q=" +title
    # print(burl)
    books = get_tx3cbg(title, burl)  # 得到json
    time.sleep(random.random() * 3)
    # print(books)
    find = False
    format = True;
    for j in range(len(books['list'])):  # 在所有json找
        id = books['list'][j]['id']
        id = int(id)
        # print(id)

        if (book_id == id):  # 可以找到详细资料的图书，把图书资料存入数据库
            # print(books['list'][j]['title'])
            # print(books['list'][j]['author'])
            # print(books['list'][j]['origAuthor'])
            find = True

            title = books['list'][j]['title']
            title = change(title)
            cover = books['list'][j]['cover']
            r = requests.get(cover, stream=True)  # stream loading
            cover = change(cover)
            # coverpic

            with open('./cover/myimage%d.png' % id, 'wb') as f:
                for chunk in r.iter_content(chunk_size=32):  # 循环写入文件，每次写入32个单位
                    f.write(chunk)

            path = './cover/myimage%d.png' % id
            # fp = open('./cover/image%d.png' % count, 'rb')
            # img = fp.read()
            coverpic = change(path)
            # coverpic = pymysql.Binary(img)
            # count += 1;
            url = books['list'][j]['url']
            # print(url)
            url = change(url)
            # print(books)
            try:
                fixedPrice = books['list'][j]['fixedPrice']
            except:
                format = False
                print("找到but格式错误：")
                print(id, books['list'][j]['title'])
                continue
            try:
                averageRating = float(avscore)
                ratingCount = int(scorenum)
            except:
                # if()
                averageRating = 0
                ratingCount = 0
            isBundle = books['list'][j]['isBundle']

            # print(type(isNew))
            isRebate = books['list'][j]['isRebate']
            wordCount = books['list'][j]['wordCount']
            # print(type(wordCount))

            fixedPrice = books['list'][j]['fixedPrice']
            salesPrice = books['list'][j]['salesPrice']
            author = books['list'][j]['author']
            author = change(str(author))
            origAuthor = books['list'][j]['origAuthor']  # 同上
            origAuthor = change(str(origAuthor))
            translator = books['list'][j]['translator']  # 同上 ，可能>0 字符串转字典？
            translator = change(str(translator))
            abstract = books['list'][j]['abstract']
            abstract = abstract.replace('"', '“')
            utf8str = abstract.encode('utf8', "replace")  # 进行编码
            abstract = utf8str.decode('utf8')  # 进行解码
            # print(str)
            # abstract = abstract.replace('"', '”')
            abstract = change(abstract)
            # abstract='"sdadafae"'

            bookspl = "INSERT INTO book(id,wordCount,averageRating,ratingCount,fixedPrice,salesPrice,author,title,abstract,cover,coverpic,origAuthor,translator,url,isRebate,isBundle)VALUES(%d,%d,%f,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d)" % (
                id, wordCount, averageRating, ratingCount, fixedPrice, salesPrice, author, title, abstract,cover,coverpic, origAuthor, translator, url, isRebate, isBundle)
            # print(book['list'][j]['editorHighligh'])
            # print(bookspl)
            # cursor.execute(bookspl)
            # db.commit()

            try:
                cursor.execute(bookspl)
                db.commit()
                for q in range(len(books['list'][j]['kinds'])):  #
                    shortid = books['list'][j]['kinds'][q]['id']
                    tmpsql = "INSERT INTO botag(tag_id,book_id)VALUES (%d,%d)" % (shortid, id)
                    cursor.execute(tmpsql)
                    #提交到数据库执行
                    db.commit()
                # book_id.append(id)
                # break

            except:
                print("-"*40)
                print("error!")
                print(bookspl)
                # print(count)
                break


def readdata(filename):
    with open(filename)as f:
        f_csv = csv.reader(f)
        print(f_csv)
        # return f_csv[0]
        for row in f_csv:
            # print(row)
            return row
            # print(type(row))

def data_write_csv(file_name, datas):#file_name为写入CSV文件的路径，datas为要写入数据列表
    file_csv = codecs.open(file_name,'w+','utf-8')#追加
    writer = csv.writer(file_csv, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    # for i in len(datas):
    #     writer.writerow(str(datas[i]))
    writer.writerow(datas)#一整个列表
    print("保存文件成功，处理结束")

if __name__=="__main__":
    db = pymysql.connect("localhost", "root", "", "recommend")

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    sql="select distinct rate.book_id from  rate where rate.book_id not in (select ID from book)"
    cursor.execute(sql)
    ids=cursor.fetchall()
    # saverate(cursor, ids[0][0])

    for i in range(len(ids)):
        print(i)

        bookid=ids[i][0]
        saverate(cursor,bookid)


    cursor.close()
    # 关闭数据库连接
    db.close()
