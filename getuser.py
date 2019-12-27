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

#爬取需要的头部信息
def get_tx3cbg(str,url):
    url = 'https://read.douban.com/j/search_v2'
    headers = {
        'Content-Type': 'application/json',
        'Cookie': '自己写',
        'Referer': 'https://read.douban.com/search?q='+urllib.parse.quote(str, safe='/', encoding=None, errors=None),
        'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        'Host': 'read.douban.com',
        'Origin': 'https: // read.douban.com'
    }
    payload={自己写}
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

#爬取数据的持久化
def saverate(cursor,userid,book_id):
    page=10
    canrate=True
    while canrate:
        userurl = "https://read.douban.com/people/" + str(userid)
        try:
            # html = urlopen("https://read.douban.com/people/66949295/ebook?start=10&sort=score", timeout=10).read().decode('utf-8')
            html = urlopen(userurl + "/ebook?start=%d&sort=score"%page, timeout=10).read().decode('utf-8')
        except:
            print("Time out")
            return
        # print(html)
        soup = BeautifulSoup(html, features='lxml')  # 用lxml解析
        span = soup.find_all("a", {"href": re.compile("/ebook/(\d{5,10})/.{4,50}")})  # 找到里面所有的书籍
        if not span:
            return
        sul = soup.find("ul", {"class": "list-lined ebook-list column-list owned-booklist"})
        sli = sul.find_all("li", {"class": "item col-media library-item"})
        # print
        count = 0;  # 同步a 和 li
        # print(span)
        # print(sli)

        for l in span:  # 得到参与的所有书籍
            # tag a 属性 href
            # print(type(l.get("href")))#str类型
            tmp = l.get("href")
            numtmp = re.findall("\d+", tmp)[0]
            numtmp = int(numtmp)  # 得到书籍的id
            # 检测书籍是否已经在数据库，如果不在存一下
            if numtmp not in book_id:  # 如果不在list中
                # print(l.get_text())
                print("没有在数据库里：%d" % numtmp)
                burl = "https://read.douban.com/search?q=" + l.get_text()
                # print(burl)
                books = get_tx3cbg(l.get_text(), burl)  # 得到json
                time.sleep(random.random() * 3)
                # print(books)
                find = False
                format = True;
                for j in range(len(books['list'])):  # 在所有json找
                    id = books['list'][j]['id']
                    id = int(id)
                    # print(id)

                    if (numtmp == id):  # 可以找到详细资料的图书，把图书资料存入数据库
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
                            averageRating = books['list'][j]['averageRating']
                            ratingCount = books['list'][j]['ratingCount']
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

                        bookspl = "INSERT INTO book(id,wordCount,averageRating,ratingCount,fixedPrice,salesPrice,author,title,abstract,cover,coverpic,origAuthor,translator,url,isRebate,isBundle)VALUES(%d,%d,%f,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d)" % (
                            id, wordCount, averageRating, ratingCount, fixedPrice, salesPrice, author, title, abstract,
                            cover,
                            coverpic, origAuthor, translator, url, isRebate, isBundle)
                        # print(book['list'][j]['editorHighligh'])
                        print(bookspl)

                        try:
                            cursor.execute(bookspl)
                            db.commit()
                            for q in range(len(books['list'][j]['kinds'])):  #
                                shortid = books['list'][j]['kinds'][q]['id']
                                tmpsql = "INSERT INTO botag(tag_id,book_id)VALUES (%d,%d)" % (shortid, id)
                                cursor.execute(tmpsql)
                                # 提交到数据库执行
                                db.commit()
                            book_id.append(id)
                            break

                        except:
                            print(bookspl)
                            print(count)
                            break

                if not format:  # 这本书一切也跳过
                    count += 1
                    continue

                # 存入成功后加入list
                if not find:
                    print(numtmp)  # 加入name和id ,做标记，表明找不到该书的一些信息
                    # 该书已被删除  则不加入这本书交互信息
                    print("not find")
                    count += 1
                    continue

            # 同时试图得到这本书的交互信息。
            bourl = sli[count].find("a", {"class": "pic"})
            # print("[[qwe",l.get_text(),sli[count].find("img")['alt'])
            # print(bourl)
            # while (re.match("/ebook/(\d{5,10})/$", bourl.get("href")) == None) and count < len(sli):
            #     #     #匹配成功
            #     count += 1#
            # 这个时候匹配成功了。
            sscore = sli[count].find("span", {"class": "text-stars"})
            score = 0  # 没有评分是0
            if sscore:
                sscore = sscore.get_text()
                score = change2num(sscore)
                print(score)
            if  score==0:#没有评分了，就不会爬第三页了
                canrate=False  # 没有评分可以放弃+页
            # else:
            #     print(score)
            # pfen

            sdate = sli[count].find("div", {"class": "bought-date"}).get_text()
            sdate = re.sub("[\u4e00-\u9fa5]", "", sdate)  # 购买日期
            sdate = int(sdate)
            print(sdate)
            table = sli[count].find("table")
            pizhu = 0
            line = 0
            mark = 0
            index = 0
            if table:
                for tr in table.findAll('tr'):  # 三大操作交互
                    for td in tr.findAll('td'):
                        if index == 0:
                            pizhu = int(td.getText())
                        elif index == 1:
                            line = int(td.getText())
                        elif index == 2:
                            mark = int(td.getText())
                        index += 1
            print(pizhu, line, mark)
            peopleid = re.findall("\d+", userurl)[0]
            peopleid = int(peopleid)
            sql = "INSERT INTO rate(people_id,book_id,pizhu,sign,mark,score,date)VALUES(%d,%d,%d,%d,%d,%f,%d)" % (
            peopleid, numtmp, pizhu, line, mark, score, sdate)
            # print(sli[count].findall("td"))
            try:
                cursor.execute(sql)
                print(sql)
                db.commit()
                # print("一本书")
            except:
                print(sql)
                print(count)
            count += 1

        page+=10

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
    base_url = "https://read.douban.com"
    res=getbookurl(cursor)
    # print(res)
    bookurl=res[0]
    bookid=res[1]
    repe=[]#people_id

    repe=readdata("peid2.csv")
    repe=str(repe[0])
    # print(booki)
    repe=repe.split(" ")
    for k in range(len(repe)):
        repe[k]=int(repe[k])

    # print("得到遍历的people名单")
    #
    #
    #
    # for i in range(len(bookurl)):#20个 已经342个人，然后每人10本书
    #     # 是否考虑存书只存放有评分的
    #     #有人不喜欢评分，但是他会买这本书
    #     #数据集多一点吧，可能会，先试一下吧
    #     bourl=base_url+bookurl[i]
    #     print(bourl)
    #     # print(bourl)#book的url
    #     # 随机从ip池中选出一个ip
    #
    #     time.sleep(random.random() * 3)
    #     repe=getpeopleurl(bourl,repe)
    #     print(i)
    #     print("现在数据规模%d"%(len(repe)))
    # print(repe)#得到的是int数字
    # print(len(repe))
    # data_write_csv("peid.csv",repe)
    print("将数据存入数据库")
    # print(repe[147])
    minus=2479
    for j in range(len(repe)-minus):
        print("-------")
        index=j+minus;
        print(repe[index])
        print("第%d位，共%d位"%(index,len(repe)))

        saverate(cursor,repe[index],bookid)
        # print(index)

    cursor.close()
    # 关闭数据库连接
    db.close()
