# TODO: CHANGE THIS FILE'S NAME TO DMA_project2_team##.py
# EX. TEAM 1 -> DMA_project2_team01.py
import mysql.connector

# TODO: REPLACE THE VALUE OF VARIABLE team (EX. TEAM 1 -> team = 1)
team = 12


def requirement1(host, user, password, directory):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    cursor.execute("""DROP DATABASE IF EXISTS DMA_team12;""")
    cursor.execute("""CREATE DATABASE DMA_team12;""")
    cursor.execute("""USE DMA_team12;""")

    #cities TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cities(
        id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NOT NULL,
        state VARCHAR(30) NOT NULL,
        country VARCHAR(30) NOT NULL,
        zip INT NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL,
        PRIMARY KEY (id));
        """)

    #gatherings TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gatherings(
        id VARCHAR(22) NOT NULL,
        topic_id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NOT NULL,
        created TIMESTAMP NOT NULL,
        description INT DEFAULT 0,
        average_rating FLOAT DEFAULT 0,
        visibility VARCHAR(15) DEFAULT 'public',
        join_mode VARCHAR(10) DEFAULT 'open',
        PRIMARY KEY (id));
        """)

    #meetings TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meetings(
        id VARCHAR(22) NOT NULL,
        gathering_id VARCHAR(22) NOT NULL,
        meeting_place_id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NOT NULL,
        created TIMESTAMP NOT NULL,
        updated TIMESTAMP NOT NULL,
        description INT DEFAULT 0,
        time TIMESTAMP NOT NULL,
        duration INT DEFAULT 0,
        fee VARCHAR(20) NULL,
        how_to_find_us INT DEFAULT 0,
        rsvp_limit INT DEFAULT -1,
        rsvp_waitlist INT DEFAULT 0,
        rsvp_accepted INT DEFAULT 0,
        PRIMARY KEY (id));
        """)

    #members TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS members(
        id VARCHAR(22) NOT NULL,
        city_id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id));
        """)

    #member_gathering TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_gathering(
        id VARCHAR(22) NOT NULL,
        member_id VARCHAR(22) NOT NULL,
        gathering_id VARCHAR(22) NOT NULL,
        description INT DEFAULT 0,
        status VARCHAR(10) DEFAULT 'prereg',
        joined TIMESTAMP NOT NULL,
        visited TIMESTAMP NOT NULL,
        PRIMARY KEY (id));
        """)

    #organizers TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS organizers(
        id VARCHAR(22) NOT NULL,
        member_id VARCHAR(22) NOT NULL,
        gathering_id VARCHAR(22) NOT NULL,
        PRIMARY KEY (id));
        """)

    #topics TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS topics(
        id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id));
        """)

    #meeting_places TABLE 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meeting_places(
        id VARCHAR(22) NOT NULL,
        city_id VARCHAR(22) NOT NULL,
        name VARCHAR(255) NULL,
        address VARCHAR(255) NULL,
        zip INT NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL,
        average_rating FLOAT DEFAULT 0,
        number_of_rating INT DEFAULT 0,
        PRIMARY KEY (id));
        """)

    #cities TABLE에 VALUE 삽입
    sql_cities = """INSERT INTO cities VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    f_cities = open(directory+'cities.csv', 'r', encoding='utf-8')
    next(f_cities)

    while True:
        line = f_cities.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_cities, line_list)
        else:
            break

    cnx.commit()
    f_cities.close()


    #gatherings TABLE에 VALUE 삽입
    sql_gatherings = """INSERT INTO gatherings VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    f_gatherings = open(directory+'gatherings.csv', 'r', encoding='utf-8')
    next(f_gatherings)

    while True:
        line = f_gatherings.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_gatherings, line_list)
        else:
            break

    cnx.commit()
    f_gatherings.close()

    #meetings TABLE에 VALUE 삽입
    sql_meetings = """INSERT INTO meetings VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    f_meetings = open(directory+'meetings.csv', 'r', encoding='utf-8')
    next(f_meetings)

    while True:
        line = f_meetings.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_meetings, line_list)
        else:
            break

    cnx.commit()
    f_meetings.close()


    #members TABLE에 VALUE 삽입
    sql_members = """INSERT INTO members VALUES (%s, %s, %s)"""
    f_members = open(directory+'members.csv', 'r', encoding='utf-8')
    next(f_members)

    while True:
        line = f_members.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_members, line_list)
        else:
            break

    cnx.commit()
    f_members.close()


    #member_gathering에 VALUE 삽입
    sql_member_gathering = """INSERT INTO member_gathering VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    f_member_gathering = open(directory+'member_gathering.csv', 'r', encoding='utf-8')
    next(f_member_gathering)

    while True:
        line = f_member_gathering.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_member_gathering, line_list)
        else:
            break

    cnx.commit()
    f_member_gathering.close()


    #organizrs TABLE에 VALUE 삽입
    sql_organizers = """INSERT INTO organizers VALUES (%s, %s, %s)"""
    f_organizers = open(directory+'organizers.csv', 'r', encoding='utf-8')
    next(f_organizers)

    while True:
        line = f_organizers.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_organizers, line_list)
        else:
            break

    cnx.commit()
    f_organizers.close()


    #topics TABLE에 VALUE 삽입
    sql_topics = """INSERT INTO topics VALUES (%s, %s)"""
    f_topics = open(directory+'topics.csv', 'r', encoding='utf-8')
    next(f_topics)

    while True:
        line = f_topics.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):
            if line_list[i] == '':
                line_list[i] = None
            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_topics, line_list)
        else:
            break

    cnx.commit()
    f_topics.close()


    #meeting_places에 VALUE 삽입
    sql_meeting_places = """INSERT INTO meeting_places VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    f_meeting_places = open(directory+'meeting_places.csv', 'r', encoding='utf-8')
    next(f_meeting_places)

    while True:
        line = f_meeting_places.readline()
        line_list = line.replace('\n', '')
        line_list = line_list.split(',')
        for i, j in enumerate(line_list):

            if line_list[i] == '':
                line_list[i] = None

            if i == 4 and line_list[4] != None:
                line_list[i] = int(float(j))

            try:
                line_list[i] = int(j)
            except:
                pass
        if line:
            cursor.execute(sql_meeting_places, line_list)
        else:
            break

    cnx.commit()
    f_meeting_places.close()


    # 각 TABLE의 제약 추가
    cursor.execute("ALTER TABLE gatherings ADD CONSTRAINT FOREIGN KEY (topic_id) REFERENCES topics(id);")
    cursor.execute("ALTER TABLE meeting_places ADD CONSTRAINT FOREIGN KEY (city_id) REFERENCES cities(id);")
    cursor.execute("ALTER TABLE meetings ADD CONSTRAINT FOREIGN KEY (gathering_id) REFERENCES gatherings(id);")
    cursor.execute("ALTER TABLE meetings ADD CONSTRAINT FOREIGN KEY (meeting_place_id) REFERENCES meeting_places(id);")
    cursor.execute("ALTER TABLE members ADD CONSTRAINT FOREIGN KEY (city_id) REFERENCES cities(id);")
    cursor.execute("ALTER TABLE member_gathering ADD CONSTRAINT FOREIGN KEY (member_id) REFERENCES members(id);")
    cursor.execute("ALTER TABLE member_gathering ADD CONSTRAINT FOREIGN KEY (gathering_id) REFERENCES gatherings(id);")
    cursor.execute("ALTER TABLE organizers ADD CONSTRAINT FOREIGN KEY (member_id) REFERENCES members(id);")
    cursor.execute("ALTER TABLE organizers ADD CONSTRAINT FOREIGN KEY (gathering_id) REFERENCES gatherings(id);")

    cursor.close()


def requirement2(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE DMA_team%02d;' % team)

    cursor.execute('''
    SELECT workson7.Year as Year, IFNULL(NewUser, 0) as NewUser, IFNULL(NewGroup, 0) as NewGroup, IFNULL(NewMeeting,0) as NewMeeting
    FROM
    (SELECT year(joined) as Year, COUNT(*) as NewUser
    FROM member_gathering
    GROUP BY year(joined)) as workson1
    RIGHT JOIN
    (SELECT Year, NewGroup, NewMeeting
    FROM ((SELECT workson2.Year, NewGroup, NewMeeting
    FROM (SELECT year(created) as Year, COUNT(*) as NewGroup
    FROM gatherings
    GROUP BY year(created)) as workson2
    LEFT JOIN (SELECT year(time) as Year, COUNT(*) as NewMeeting
    FROM meetings
    GROUP BY year(time)) as workson3
    ON workson2.Year = workson3.Year)
    UNION
    (SELECT workson5.Year, NewGroup, NewMeeting
    FROM (SELECT year(created) as Year, COUNT(*) as NewGroup
    FROM gatherings
    GROUP BY year(created)) as workson4
    RIGHT JOIN (SELECT year(time) as Year, COUNT(*) as NewMeeting
    FROM meetings
    GROUP BY year(time)) as workson5
    ON workson4.Year = workson5.Year)
    ) as workson6
    ) as workson7
    ON workson1.Year = workson7.Year
    ORDER BY Year
    ''')


    fopen = open('project2_team%02d_req2.txt' % team, 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()
    cursor.close()


def requirement3(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE DMA_team%02d;' % team)

    cursor.execute("""
        SELECT MP.id as id, city_id, name, address
        FROM meeting_places as MP, (SELECT view1.id as id
        FROM (SELECT id
        FROM meeting_places
        WHERE number_of_rating>=5 AND average_rating>=4.5) as view1
        LEFT JOIN (SELECT meeting_place_id as id
        FROM meetings
        GROUP BY meeting_place_id
        HAVING COUNT(*)>3) as view2
        ON view1.id=view2.id
        WHERE view2.id is NULL) as view3
        WHERE MP.id=view3.id
        ORDER BY id;
        """)

    fopen = open('project2_team%02d_req3.txt' % team, 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()
    cursor.close()


def requirement4(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE DMA_team%02d;' % team)

    cursor.execute("""
        SELECT id, city_id, name
        FROM members as m
        WHERE m.id IN (SELECT member_id
        FROM member_gathering
        WHERE TIMESTAMPDIFF(YEAR, joined, visited) >= 1
        GROUP BY member_id
        HAVING COUNT(gathering_id) >= 5)  
        ORDER BY id; 
        """)

    fopen = open('project2_team%02d_req4.txt' % team, 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()
    cursor.close()


def requirement5(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE DMA_team%02d;' % team)

    cursor.execute("""
        ALTER TABLE gatherings ADD remarks VARCHAR(20);
            """)

    cursor.execute("""
        UPDATE gatherings SET remarks = CASE WHEN year(created)>=2014 AND id NOT IN (SELECT DISTINCT gathering_id
        FROM member_gathering) THEN "need promotion" 
        WHEN year(created)<2014 AND id NOT IN (SELECT DISTINCT gathering_id
        FROM member_gathering) THEN "to be deleted" END;
            """)

    cnx.commit()

    cursor.execute("""       
        SELECT id, name, created, remarks
        FROM gatherings
        WHERE id NOT IN (SELECT DISTINCT gathering_id
        FROM member_gathering)
        ORDER BY id;
        """)

    fopen = open('project2_team%02d_req5.txt' %team, 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()
    cursor.close()


def requirement6(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE DMA_team%02d;' % team)

    cursor.execute("""   
            SELECT T.name, COUNT(g.id) as total
            FROM gatherings as G, topics as T
            WHERE G.average_rating >= 4.8 AND G.topic_id = T.id
            GROUP BY T.id
            ORDER BY total DESC;
            """)

    fopen = open('project2_team%02d_req6.txt' % team, 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()
    cursor.close()


# TODO: REPLACE THE VALUES OF FOLLOWING VARIABLES
host = 'localhost'
user = 'root'
password = '01094321205'
directory_in = 'C:/Users/parks/Desktop/'


#requirement1(host=host, user=user, password=password, directory=directory_in)
#requirement2(host=host, user=user, password=password)
requirement3(host=host, user=user, password=password)
#requirement4(host=host, user=user, password=password)
#requirement5(host=host, user=user, password=password)
#requirement6(host=host, user=user, password=password)

