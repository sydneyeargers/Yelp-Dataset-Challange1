import json
import sys
from PyQt5.QtWidgets import QMainWindow, QApplication, QTableWidgetItem
from PyQt5 import uic
import psycopg2

conn = None


def cleanStr4SQL(s):
    return s.replace("'", "`").replace("\n", " ")


def int2BoolStr(value):
    if value == 0:
        return 'False'
    else:
        return 'True'


def getConn():
    global conn
    if conn is None:
        conn = psycopg2.connect(database="Milestone2DB", user="postgres", host="localhost", password="",
                                port="5432")
    return conn


def insert2BusinessTable():
    # reading the JSON file
    with open('/Users/sydneyyeargers/Downloads/Yelp-CptS451/yelp_business.JSON', 'r') as f:
        line = f.readline()
        count_line = 0
        innerconn = getConn()
        while line:
            data = json.loads(line)
            writeBusiness(data, innerconn)
            innerconn.commit()
            line = f.readline()
            count_line += 1
    print(count_line)
    f.close()


def writeBusiness(data, localconn):
    sql_str = "INSERT INTO BUSINESS (business_id, name, neighborhood, address, city, state, postal_code, " \
              "latitude, longitude, stars, review_count, is_open, num_checkins, review_rating) " \
              "VALUES ('" + cleanStr4SQL(data['business_id']) + "','" + cleanStr4SQL(data["name"]) + "','" + \
              cleanStr4SQL(data["neighborhood"]) + "','" + cleanStr4SQL(data["address"]) + "','" + \
              cleanStr4SQL(data["city"]) + "','" + cleanStr4SQL(data["state"]) + "','" + \
              cleanStr4SQL(data["postal_code"]) + "'," + str(data["latitude"]) + "," + str(data["longitude"]) + \
              "," + str(data["stars"]) + "," + str(data["review_count"]) + "," + int2BoolStr(data["is_open"]) + \
              ",0 " + ",0.0 " + ");"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to BUSINESS failed!")
        print(e)
    # getAttributes(data, localconn)
    getCategory(data, localconn)
    # getHours(data, localconn)


'''
def getAttributes(data, localconn):
    attributes = data['attributes']
    for pk, pv in attributes.items():
        if type(pv) is dict:
            for ppk, ppv in pv.items():
                writeAttributes(data['business_id'], pk, ppk, ppv, localconn)
        else:
            writeAttributes(data['business_id'], pk, "", pv, localconn)


def writeAttributes(business_id, attr_primary, attr_secondary, attr_value, localconn):
    sql_str = "INSERT INTO ATTRIBUTES (business_id, attr_name, sub_attr, attr_value) VALUES ('" + \
              cleanStr4SQL(business_id) + "','" + cleanStr4SQL(attr_primary) + "','" + \
              cleanStr4SQL(str(attr_secondary)) + "','" + cleanStr4SQL(str(attr_value)) + "');"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to ATTRIBUTES failed!")
        print(e)
'''


def getCategory(data, localconn):
    categories = data['categories']
    for category in categories:
        writeCategory(data['business_id'], category, localconn)


def writeCategory(business_id, category_name, localconn):
    sql_str = "INSERT INTO CATEGORY (business_id, category_name) " \
              "VALUES ('" + cleanStr4SQL(business_id) + "','" + cleanStr4SQL(category_name) + "');"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to CATEGORY failed!")
        print(e)


'''
def getHours(data, localconn):
    hours = data['hours']
    for day, hour in hours.items():
        writeHours(data['business_id'], day, hour, localconn)


def writeHours(business_id, weekday, hours, localconn):
    sql_str = "INSERT INTO HOURS (business_id, weekday, hours)" \
              "VALUES ('" + cleanStr4SQL(business_id) + "','" + cleanStr4SQL(weekday) + "','" + \
              cleanStr4SQL(hours) + "');"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to HOURS failed!")
        print(e)
'''


def insert2UsersTable():
    with open('/Users/sydneyyeargers/Downloads/Yelp-CptS451/yelp_user.JSON', 'r') as f:
        line = f.readline()
        count_line = 0
        innerconn = getConn()
        while line:
            data = json.loads(line)
            writeUsers(data, innerconn)
            innerconn.commit()
            line = f.readline()
            count_line += 1
    print(count_line)
    f.close()


def writeUsers(data, localconn):
    sql_str = "INSERT INTO USERS (user_id, average_stars, yelping_since, user_name, review_count, useful, funny, cool," \
              "fans, compliment_cool, compliment_cute, compliment_funny, compliment_hot, compliment_list, " \
              "compliment_more, compliment_note, compliment_photos, compliment_plain, compliment_profile, " \
              "compliment_writer)" \
              "VALUES ('" + cleanStr4SQL(data['user_id']) + "'," + str(data['average_stars']) + "," + \
              str(data['yelping_since']) + ",'" + cleanStr4SQL(data['name']) + "'," + str(data['review_count']) + "," + \
              str(data['useful']) + "," + str(data['funny']) + "," + str(data['cool']) + "," + str(data['fans']) + "," \
              + str(data['compliment_cool']) + "," + str(data['compliment_cute']) + "," + str(data['compliment_funny']) \
              + "," + str(data['compliment_hot']) + "," + str(data['compliment_list']) + "," + \
              str(data['compliment_more']) + "," + str(data['compliment_note']) + "," + str(data['compliment_photos']) \
              + "," + str(data['compliment_plain']) + "," + str(data['compliment_profile']) + "," + \
              str(data['compliment_writer']) + ");"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to USER failed!")
        print(e)
    # getElite(data, localconn)
    # getFriends(data, localconn)


'''
def getElite(data, localconn):
    elite = data['elite']
    for year in elite:
        writeElite(data['user_id'], year, localconn)


def writeElite(user_id, year, localconn):
    sql_str = "INSERT INTO ELITE (user_id, elite_year) " \
              "VALUES ('" + str(user_id) + "'," + str(year) + ");"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to ELITE failed!")
        print(e)


def getFriends(data, localconn):
    friends = data['friends']
    for friend in friends:
        writeFriends(data['user_id'], friend, localconn)


def writeFriends(user_id, friend_id, localconn):
    sql_str = "INSERT INTO FRIENDS (user_id, friend_id) " \
              "VALUES ('" + str(user_id) + "','" + str(friend_id) + "');"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to FRIENDS failed!")
        print(e)
'''


def insert2CheckinsTable():
    with open('/Users/sydneyyeargers/Downloads/Yelp-CptS451/yelp_checkin.JSON', 'r') as f:
        line = f.readline()
        count_line = 0
        innerconn = getConn()
        while line:
            data = json.loads(line)
            getCheckins(data, innerconn)
            innerconn.commit()
            line = f.readline()
            count_line += 1
    print(count_line)
    f.close()


def getCheckins(data, localconn):
    checkins = data['time']
    for day, hours in checkins.items():
        for hour, count in hours.items():
            writeCheckins(data['business_id'], hour, day, count, localconn)


def writeCheckins(business_id, hour, day, count, localconn):
    sql_str = "INSERT INTO CHECK_INS (business_id, time, weekday, checkin_no)" \
              "VALUES ('" + cleanStr4SQL(business_id) + "','" + cleanStr4SQL(hour) + "','" + \
              cleanStr4SQL(day) + "','" + str(count) + "');"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to CHECK-INS failed!")
        print(e)


def insert2ReviewTable():
    with open('/Users/sydneyyeargers/Downloads/Yelp-CptS451/yelp_review.JSON', 'r') as f:
        line = f.readline()
        count_line = 0
        innerconn = getConn()
        while line:
            data = json.loads(line)
            writeReview(data, innerconn)
            innerconn.commit()
            line = f.readline()
            count_line += 1
    print(count_line)
    f.close()


def writeReview(data, localconn):
    sql_str = "INSERT INTO REVIEW (review_id, user_id, business_id, stars, date, text, useful, funny, cool) " \
              "VALUES ('" + cleanStr4SQL(data['review_id']) + "','" + cleanStr4SQL(data['user_id']) + "','" + \
              cleanStr4SQL(data['business_id']) + "'," + str(data['stars']) + "," + \
              str(data['date']) + ",'" + cleanStr4SQL(data['text']) + "'," + \
              str(data['useful']) + "," + str(data['funny']) + "," + str(data['cool']) + ");"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        cur.close()
    except Exception as e:
        print("Insert to REVIEW failed!")
        print(e)


def addIndexes(localconn):
    sql_str = "CREATE INDEX review_business_id_index on review (business_id); " \
              "CREATE INDEX check_ins_business_id_index on check_ins (business_id); " \
              "CREATE INDEX bizz_postal_index on business (postal_code);"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        localconn.commit()
        cur.close()
    except Exception as e:
        print("Indexing failed!")
        print(e)


def updateNumCheckins(localconn):
    sql_str = "UPDATE BUSINESS SET num_checkins = (SELECT SUM(checkin_no) FROM CHECK_INS WHERE " \
              "BUSINESS.business_id = CHECK_INS.business_id GROUP BY CHECK_INS.business_id);"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        localconn.commit()
        cur.close()
    except Exception as e:
        print("Update of num_checkins failed!")
        print(e)


def updateReviewCount(localconn):
    sql_str = "UPDATE BUSINESS SET review_count = (SELECT COUNT(DISTINCT review_id) FROM REVIEW WHERE " \
              "BUSINESS.business_id = REVIEW.business_id GROUP BY REVIEW.business_id);"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        localconn.commit()
        cur.close()
    except Exception as e:
        print("Update of review_count failed!")
        print(e)


def updateReviewRating(localconn):
    sql_str = "UPDATE BUSINESS SET review_rating = (SELECT AVG(stars) FROM REVIEW WHERE " \
              "BUSINESS.business_id = REVIEW.business_id GROUP BY REVIEW.business_id);"
    try:
        cur = localconn.cursor()
        cur.execute(sql_str)
        localconn.commit()
        cur.close()
    except Exception as e:
        print("Update of review_rating failed!")
        print(e)


qtCreatorFile = "Milestone3.ui"
Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)


class milestone3(QMainWindow):
    def __init__(self):
        super(milestone3, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.loadStateList()
        self.ui.stateList.currentTextChanged.connect(self.stateChanged)
        self.ui.cityList.itemSelectionChanged.connect(self.cityChanged)
        self.ui.zipList.itemSelectionChanged.connect(self.zipChanged)
        self.ui.categoryList.itemSelectionChanged.connect(self.categoryChanged)
        self.ui.searchButton.clicked.connect(self.searchButtonPressed)
        self.ui.busButton.clicked.connect(self.busButtonPressed)

    def executeQuery(self, sql_str):
        try:
            conn = psycopg2.connect(database="Milestone2DB", user="postgres", host="localhost",
                                    password="SQL4.Passwordd", port="5432")
            print('Database successfully connected!')
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()
        cur.execute(sql_str)
        conn.commit()
        result = cur.fetchall()
        conn.close()
        return result

    def clearTableDisplays(self):
        for i in reversed(range(self.ui.businessTable.rowCount())):
            self.ui.businessTable.removeRow(i)
        for i in reversed(range(self.ui.popularTable.rowCount())):
            self.ui.popularTable.removeRow(i)
        for i in reversed(range(self.ui.successfulTable.rowCount())):
            self.ui.successfulTable.removeRow(i)

    def loadStateList(self):
        self.ui.stateList.clear()
        sql_str = "SELECT distinct state FROM BUSINESS ORDER BY state;"
        try:
            results = self.executeQuery(sql_str)
            for row in results:
                self.ui.stateList.addItem(row[0])
        except:
            print('Query failed!')
        self.ui.stateList.setCurrentIndex(-1)
        self.ui.stateList.clearEditText()

    def stateChanged(self):
        self.ui.cityList.clear()
        if self.ui.stateList.currentIndex() >= 0:
            state = self.ui.stateList.currentText()
            sql_str = "SELECT distinct city FROM BUSINESS WHERE state ='" + state + "' ORDER BY city;"
            try:
                results = self.executeQuery(sql_str)
                for row in results:
                    self.ui.cityList.addItem(row[0])
            except:
                print("Query has failed (55)")
        self.clearTableDisplays()

    def cityChanged(self):
        self.ui.zipList.clear()
        if (self.ui.stateList.currentIndex() >= 0) and (len(self.ui.cityList.selectedItems()) > 0):
            state = self.ui.stateList.currentText()
            city = self.ui.cityList.selectedItems()[0].text()
            sql_str = "SELECT distinct postal_code FROM BUSINESS WHERE state ='" + state + "' AND city='" + city + \
                      "' ORDER BY postal_code;"
            try:
                results = self.executeQuery(sql_str)
                for row in results:
                    self.ui.zipList.addItem(row[0])
            except:
                print("Query has failed (367)")
        self.clearTableDisplays()

    def zipChanged(self):
        self.ui.categoryList.clear()
        if (self.ui.stateList.currentIndex() >= 0) and (len(self.ui.cityList.selectedItems()) > 0) and \
                (len(self.ui.zipList.selectedItems()) > 0):
            state = self.ui.stateList.currentText()
            city = self.ui.cityList.selectedItems()[0].text()
            zipcode = self.ui.zipList.selectedItems()[0].text().strip()
            sql_str = "SELECT distinct category_name FROM CATEGORY, BUSINESS WHERE " \
                      "CATEGORY.business_id = BUSINESS.business_id AND state ='" + state + "' AND city='" + city + \
                      "' AND postal_code ='" + zipcode + "' ORDER BY category_name;"
            zip_biz_count_sql_str = "SELECT cast(count(distinct business_id) as varchar(5)) FROM business " \
                                    "WHERE postal_code ='" + zipcode + "';"
            zip_total_pop_sql_str = "SELECT cast(population as varchar(10)) FROM zipcodeData WHERE zipcode ='" + zipcode + "';"
            zip_avg_income_sql_str = "SELECT cast(meanIncome as varchar(10)) FROM zipcodeData WHERE zipcode ='" + zipcode + "';"
            try:
                results = self.executeQuery(sql_str)
                num_biz_results = self.executeQuery(zip_biz_count_sql_str)
                total_pop_results = self.executeQuery(zip_total_pop_sql_str)
                avg_income_results = self.executeQuery(zip_avg_income_sql_str)
                for row in results:
                    self.ui.categoryList.addItem(row[0])
                for row in num_biz_results:
                    self.ui.numBusiness.setText(row[0])
                for row in total_pop_results:
                    self.ui.totalPop.setText(row[0])
                for row in avg_income_results:
                    self.ui.avgIncome.setText(row[0])
            except Exception as e:
                print("Query has failed (408)")
                print(e)
            self.clearTableDisplays()

    def categoryChanged(self):
        if (self.ui.stateList.currentIndex() >= 0) and (len(self.ui.cityList.selectedItems()) > 0) and \
                (len(self.ui.zipList.selectedItems()) > 0) and (len(self.ui.categoryList.selectedItems()) > 0):
            state = self.ui.stateList.currentText()
            city = self.ui.cityList.selectedItems()[0].text()
            zipcode = self.ui.zipList.selectedItems()[0].text()
            category = self.ui.categoryList.selectedItems()[0].text()
            for i in reversed(range(self.ui.businessTable.rowCount())):
                self.ui.businessTable.removeRow(i)
            sql_str = "SELECT name, CONCAT(address,' ',city,', ',state,' ',postal_code), CAST(stars as VARCHAR(6)), " \
                      "CAST(review_count as VARCHAR(6)), cast(review_rating as VARCHAR(6)), " \
                      "CAST(num_checkins as VARCHAR(6)) FROM BUSINESS, CATEGORY WHERE CATEGORY.business_id = " \
                      "BUSINESS.business_id AND state ='" + state + "' AND city='" + city + \
                      "' AND postal_code ='" + zipcode + "' AND category_name ='" + category + "' ORDER BY name;"
            self.executeQuery(sql_str)
            try:
                results = self.executeQuery(sql_str)
                style = "::section {""background-color: #f3f3f3; }"
                self.ui.businessTable.horizontalHeader().setStyleSheet(style)
                self.ui.businessTable.setColumnCount(len(results[0]))
                self.ui.businessTable.setRowCount(len(results))
                self.ui.businessTable.setHorizontalHeaderLabels(['Business Name', 'Address', 'Rating', '# Reviews',
                                                                 'Avg. rating', '# of Check-ins'])
                self.ui.businessTable.resizeColumnsToContents()
                self.ui.businessTable.setColumnWidth(0, 150)
                self.ui.businessTable.setColumnWidth(1, 150)
                self.ui.businessTable.setColumnWidth(2, 50)
                self.ui.businessTable.setColumnWidth(3, 50)
                self.ui.businessTable.setColumnWidth(4, 50)
                self.ui.businessTable.setColumnWidth(5, 50)
                currentRowCount = 0

                for row in results:
                    for colCount in range(0, len(results[0])):
                        self.ui.businessTable.setItem(currentRowCount, colCount, QTableWidgetItem(row[colCount]))
                    currentRowCount += 1
            except Exception as e:
                print("Query has failed (433)")
                print(e)

    def searchButtonPressed(self):
        if (self.ui.stateList.currentIndex() >= 0) and (len(self.ui.cityList.selectedItems()) > 0) and \
                (len(self.ui.zipList.selectedItems()) > 0) and (len(self.ui.categoryList.selectedItems()) > 0):
            state = self.ui.stateList.currentText()
            city = self.ui.cityList.selectedItems()[0].text()
            zipcode = self.ui.zipList.selectedItems()[0].text()
            category = self.ui.categoryList.selectedItems()[0].text()
            for i in reversed(range(self.ui.businessTable.rowCount())):
                self.ui.businessTable.removeRow(i)
            sql_str = "SELECT name, CONCAT(address,' ',city,', ',state,' ',postal_code), CAST(stars as VARCHAR(6)), " \
                      "CAST(review_count as VARCHAR(6)), cast(review_rating as VARCHAR(6)), " \
                      "CAST(num_checkins as VARCHAR(6)) FROM BUSINESS, CATEGORY WHERE CATEGORY.business_id = " \
                      "BUSINESS.business_id AND state ='" + state + "' AND city='" + city + \
                      "' AND postal_code ='" + zipcode + "' AND category_name ='" + category + "' ORDER BY name;"
            self.executeQuery(sql_str)
            try:
                results = self.executeQuery(sql_str)
                style = "::section {""background-color: #f3f3f3; }"
                self.ui.businessTable.horizontalHeader().setStyleSheet(style)
                self.ui.businessTable.setColumnCount(len(results[0]))
                self.ui.businessTable.setRowCount(len(results))
                self.ui.businessTable.setHorizontalHeaderLabels(['Business Name', 'Address', 'Rating', '# Reviews',
                                                                 'Avg. rating', '# of Check-ins'])
                self.ui.businessTable.resizeColumnsToContents()
                self.ui.businessTable.setColumnWidth(0, 150)
                self.ui.businessTable.setColumnWidth(1, 150)
                self.ui.businessTable.setColumnWidth(2, 50)
                self.ui.businessTable.setColumnWidth(3, 50)
                self.ui.businessTable.setColumnWidth(4, 50)
                self.ui.businessTable.setColumnWidth(5, 50)
                currentRowCount = 0

                for row in results:
                    for colCount in range(0, len(results[0])):
                        self.ui.businessTable.setItem(currentRowCount, colCount, QTableWidgetItem(row[colCount]))
                    currentRowCount += 1
            except Exception as e:
                print("Query has failed (433)")
                print(e)
        elif (self.ui.stateList.currentIndex() >= 0) and (len(self.ui.cityList.selectedItems()) > 0) and \
                (len(self.ui.zipList.selectedItems()) > 0) and (len(self.ui.categoryList.selectedItems()) <= 0):
            state = self.ui.stateList.currentText()
            city = self.ui.cityList.selectedItems()[0].text()
            zipcode = self.ui.zipList.selectedItems()[0].text()
            for i in reversed(range(self.ui.businessTable.rowCount())):
                self.ui.businessTable.removeRow(i)
            sql_str = "SELECT name, CONCAT(address,' ',city,', ',state,' ',postal_code), CAST(stars as VARCHAR(6)), " \
                      "CAST(review_count as VARCHAR(6)), cast(review_rating as VARCHAR(6)), " \
                      "CAST(num_checkins as VARCHAR(6)) FROM BUSINESS WHERE state ='" + state + "' AND city='" + city + \
                      "' AND postal_code ='" + zipcode + "' ORDER BY name;"
            self.executeQuery(sql_str)
            try:
                results = self.executeQuery(sql_str)
                style = "::section {""background-color: #f3f3f3; }"
                self.ui.businessTable.horizontalHeader().setStyleSheet(style)
                self.ui.businessTable.setColumnCount(len(results[0]))
                self.ui.businessTable.setRowCount(len(results))
                self.ui.businessTable.setHorizontalHeaderLabels(['Business Name', 'Address', 'Rating', '# Reviews',
                                                                 'Avg. rating', '# of Check-ins'])
                self.ui.businessTable.resizeColumnsToContents()
                self.ui.businessTable.setColumnWidth(0, 150)
                self.ui.businessTable.setColumnWidth(1, 150)
                self.ui.businessTable.setColumnWidth(2, 50)
                self.ui.businessTable.setColumnWidth(3, 50)
                self.ui.businessTable.setColumnWidth(4, 50)
                self.ui.businessTable.setColumnWidth(5, 50)
                currentRowCount = 0

                for row in results:
                    for colCount in range(0, len(results[0])):
                        self.ui.businessTable.setItem(currentRowCount, colCount, QTableWidgetItem(row[colCount]))
                    currentRowCount += 1
            except Exception as e:
                print("Query has failed (433)")
                print(e)
        else:
            print("check selections")

    def busButtonPressed(self):
        if len(self.ui.zipList.selectedItems()) > 0:
            zipcode = self.ui.zipList.selectedItems()[0].text()
            for i in reversed(range(self.ui.popularTable.rowCount())):
                self.ui.popularTable.removeRow(i)
            for i in reversed(range(self.ui.successfulTable.rowCount())):
                self.ui.successfulTable.removeRow(i)
            sql_str_pop = "SELECT RANK() OVER(PARTITION BY bn.postal_code ORDER BY " \
                          "(coalesce(num_checkins,0) - avg_checkins) desc) as Rank, name, " \
                          "cast(coalesce(num_checkins,0) as varchar(6)) as num_checkins, cast(stars as varchar(5)) " \
                          "FROM business as bn INNER JOIN (SELECT postal_code, avg(num_checkins) as avg_checkins " \
                          "FROM business as b GROUP BY postal_code ORDER BY postal_code) as temp " \
                          "ON bn.postal_code = temp.postal_code WHERE bn.postal_code ='" + zipcode + "' ORDER BY rank;"
            sql_str_success = "SELECT temp1.name, cast(temp1.review_rating as varchar(6)), " \
                              "cast(temp3.num_checkins as varchar(6)) FROM (SELECT bs.business_id, bs.name, " \
                              "bs.review_rating, bs.postal_code, (review_rating - avg_review) as review_diff" \
                              " FROM business as bs, (SELECT cast(avg(review_rating) as decimal(5,2)) as avg_review " \
                              "FROM BUSINESS as bsn) as temp0) as temp1 INNER JOIN (SELECT business_id, name," \
                              " coalesce(num_checkins,0) as num_checkins, cast(avg_checkins as decimal(10,2)), " \
                              "cast((coalesce(num_checkins,0) - avg_checkins) as decimal(10,2)) as checkins_diff " \
                              "FROM business as bn INNER JOIN (SELECT postal_code, avg(num_checkins) as avg_checkins " \
                              "FROM business as b GROUP BY postal_code ORDER BY postal_code) as temp2 " \
                              "ON temp2.postal_code = bn.postal_code)as temp3 ON temp3.business_id = temp1.business_id" \
                              " WHERE review_diff > 0 AND postal_code ='" + zipcode + "' ORDER BY review_diff desc, " \
                                                                                      "checkins_diff desc;"
            self.executeQuery(sql_str_pop)
            self.executeQuery(sql_str_success)
            try:
                pop_results = self.executeQuery(sql_str_pop)
                succ_results = self.executeQuery(sql_str_success)
                style = "::section {""background-color: #f3f3f3; }"
                self.ui.popularTable.horizontalHeader().setStyleSheet(style)
                self.ui.popularTable.setColumnCount(len(pop_results[0]))
                self.ui.popularTable.setRowCount(len(pop_results))
                self.ui.popularTable.setHorizontalHeaderLabels([' ', 'Business Name', '# of Check-ins', 'Stars'])
                self.ui.popularTable.resizeColumnsToContents()
                self.ui.popularTable.setColumnWidth(0, 25)
                self.ui.popularTable.setColumnWidth(1, 175)
                self.ui.popularTable.setColumnWidth(2, 75)
                self.ui.popularTable.setColumnWidth(3, 75)
                self.ui.successfulTable.horizontalHeader().setStyleSheet(style)
                self.ui.successfulTable.setColumnCount(len(succ_results[0]))
                self.ui.successfulTable.setRowCount(len(succ_results))
                self.ui.successfulTable.setHorizontalHeaderLabels(['Business Name', 'Review Rating', '# of Check-ins'])
                self.ui.successfulTable.resizeColumnsToContents()
                self.ui.successfulTable.setColumnWidth(0, 175)
                self.ui.successfulTable.setColumnWidth(1, 100)
                self.ui.successfulTable.setColumnWidth(2, 100)
                pop_currentRowCount = 0
                succ_currentRowCount = 0
                for row in pop_results:
                    for colCount in range(0, len(pop_results[0])):
                        self.ui.popularTable.setItem(pop_currentRowCount, colCount, QTableWidgetItem(row[colCount]))
                    pop_currentRowCount += 1
                for row in succ_results:
                    for colCount in range(0, len(succ_results[0])):
                        self.ui.successfulTable.setItem(succ_currentRowCount, colCount, QTableWidgetItem(row[colCount]))
                    succ_currentRowCount += 1
            except Exception as e:
                print("Query has failed (515)")
                print(e)


def insertToTables():
    insert2BusinessTable()
    insert2UsersTable()
    insert2CheckinsTable()
    insert2ReviewTable()


def updateTables():
    addIndexes(getConn())
    updateNumCheckins(getConn())
    updateReviewCount(getConn())
    updateReviewRating(getConn())


if __name__ == "__main__":
    insertToTables()
    updateTables()
    app = QApplication(sys.argv)
    window = milestone3()
    window.show()
    sys.exit(app.exec_())
