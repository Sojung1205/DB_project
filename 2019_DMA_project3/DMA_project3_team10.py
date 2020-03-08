# TODO: IMPORT LIBRARIES NEEDED FOR PROJECT 3
import mysql.connector
import os
import pandas as pd
from sklearn import tree
import graphviz
import numpy as np
from mlxtend.frequent_patterns import association_rules, apriori


# TODO: CHANGE GRAPHVIZ DIRECTORY
os.environ["PATH"] += os.pathsep + 'C:/Program Files (x86)/Graphviz2.38/bin/'

# TODO: CHANGE MYSQL INFORMATION
HOST = 'localhost'
USER = 'root'
PASSWORD = '01094321205'
SCHEMA = 'DMA_team12'


def part1():
    cnx = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE %s;' % SCHEMA)

    # TODO: REQUIREMENT 1. WRITE MYSQL QUERY IN EXECUTE FUNCTION BELOW

    cursor.execute("""
            ALTER TABLE gatherings ADD hot_gathering INT(1) DEFAULT 0;;
                """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Hot_Gatherings_List(
        id VARCHAR(22) NOT NULL,
        PRIMARY KEY (id));""")
    #hot_gatherings.txt 파일의 내용을 담기 위한 새로운 테이블을 데이터베이스에 생성


    sql_hot_gatherings_list = """INSERT INTO Hot_Gatherings_List VALUES (%s)"""

    f = open('C:/Users/parks/Desktop/Hot_Gatherings_List.txt', 'r', encoding='utf-8')

    while True:
        line_list = []
        line = f.readline()
        line = line.replace('\n', '')
        line_list.append(line)
        if line:
            cursor.execute(sql_hot_gatherings_list, line_list)
        else:
            break

    cnx.commit()
    f.close()

    cursor.execute("""
        UPDATE gatherings SET hot_gathering = CASE 
        WHEN id IN (SELECT id
        FROM Hot_Gatherings_List) THEN 1 ELSE 0 END;
        """)
    #새로 추가한 hot_gathering column을 update. Hot_Gatherings_List로 위에서 생성한 TABLE 안에 포함되어 있으면 (주어진 hot_gatherings의 txt파일에 포함되어 있으면) 1, 없으면 0 할당

    cnx.commit()

    cursor.execute("""
    SELECT view5.gathering_id, hot_gathering, age, description, average_rating, number_of_members, number_of_meetings, number_of_recently_joined_members
    FROM (SELECT view3.gathering_id, hot_gathering, age, description, average_rating, number_of_members, number_of_meetings
    FROM(SELECT view1.gathering_id, hot_gathering, age, description, average_rating, number_of_members
    FROM
    (SELECT id as gathering_id, hot_gathering, TIMESTAMPDIFF(HOUR, created, '2018-01-01') as age, description, average_rating
    FROM gatherings
    WHERE year(created) < 2018) as view1 
    LEFT JOIN
    (SELECT gathering_id, COUNT(member_id) as number_of_members
    FROM member_gathering 
    GROUP BY gathering_id) as view2
    ON view1.gathering_id = view2.gathering_id) as view3
    LEFT JOIN
    (SELECT gathering_id, COUNT(meeting_place_id) as number_of_meetings
    FROM meetings
    WHERE year(created) < 2018 
    GROUP BY gathering_id) as view4
    ON view3.gathering_id = view4.gathering_id)  as view5
    LEFT JOIN
    (SELECT gathering_id, COUNT(member_id) as number_of_recently_joined_members
    FROM member_gathering 
    WHERE TIMESTAMPDIFF(YEAR, joined, '2018-01-01')<3 ##여기 수정
    GROUP BY gathering_id) as view6
    ON view5.gathering_id = view6.gathering_id
    ORDER BY gathering_id    """)
    #requirement1에 맞는 애들을 select하는 쿼리문
    #쿼리문의 결과를 txt파일에 저장(project2 때처럼)

    fopen = open('project3_team10_data.txt', 'w', encoding='utf8')

    rows = cursor.fetchall()
    for line in rows:
        for i in range(len(line)):
            fopen.write(str(line[i]))
            if i < len(line) - 1: fopen.write(';')
        if line != rows[len(rows) - 1]: fopen.write('\n')

    fopen.close()

    # ----------------------------------------------------------------

    # TODO: REQUIREMENT 2. MAKE AND SAVE DECISION TREES

    #저장했던 txt파일을 pandas 모듈을 이용해 읽어서 바로 dataframe형식으로 만듦
    data_table = pd.read_table('project3_team10_data.txt', delimiter=';',
                               names=['gathering_id', 'hot_gathering', 'age', 'description', 'average_rating',
                                      'number_of_members', 'number_of_meetings', 'number_of_recently_joined_members'])

    #label이 되는 class는 df형식으로 decision tree를 만들 수 없기 때문에 array형식으로 reshape해줌.
    classes = data_table['hot_gathering'].values.reshape(-1, 1)

    #필요한 컬럼만 남기고 모두 없애고, txt파일을 읽어서 null값이 그대로 문자열 'None'으로 읽히는데, 이것들을 모두 숫자 0으로 바꿈. (decisiontreeclassifier에는 숫자형만 넣어야 함)
    features = data_table.drop(['hot_gathering', 'gathering_id'], axis=1)
    features['number_of_meetings'] = features['number_of_meetings'].replace('None', 0).astype(int)
    features['number_of_members'] = features['number_of_members'].replace('None', 0).astype(int)
    features['number_of_recently_joined_members'] = features['number_of_recently_joined_members'].replace('None',0).astype(int)

    #TA07에 나온 대로 classifier 두 가지 방식으로 생성
    DT_gini = tree.DecisionTreeClassifier(criterion='gini', min_samples_leaf=10, max_depth=5)
    DT_gini.fit(X=features, y=classes) #samples leaf ~

    graph_gini = tree.export_graphviz(DT_gini, out_file=None,
                                      feature_names=['age', 'description', 'average_rating', 'number_of_members',
                                                     'number_of_meetings', 'number_of_recently_joined_members'],
                                      class_names=['normal', 'HOT'])
    graph_gini = graphviz.Source(graph_gini)
    graph_gini.render('DMA_project3_team10_gini', view=True)

    DT_entropy = tree.DecisionTreeClassifier(criterion='entropy', min_samples_leaf=10, max_depth=5)
    DT_entropy.fit(X=features, y=classes)

    graph_entropy = tree.export_graphviz(DT_entropy, out_file=None,
                                         feature_names=['age', 'description', 'average_rating', 'number_of_members',
                                                        'number_of_meetings', 'number_of_recently_joined_members'],
                                         class_names=['normal', 'HOT'])
    graph_entropy = graphviz.Source(graph_entropy)
    graph_entropy.render('DMA_project3_team10_entropy', view=True)

    # -------------------------------------------------
    cursor.close()


def part2():
    cnx = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE %s;' % SCHEMA)
    cursor.execute('DROP VIEW IF EXISTS maybe_NY_gatherings;')
    cursor.execute('DROP VIEW IF EXISTS NY_gatherings;')

    # TODO: REQUIREMENT 4. WRITE MYSQL QUERY IN EXECUTE FUNCTIONS BELOW

    # Query on Members who live in NY
    cursor.execute('''
        CREATE OR REPLACE VIEW M
        AS SELECT id, city_id 
        FROM members 
        WHERE city_id = 10001 
        ''')

    # Gatherings that have 99% of members live in NY
    # (SUM(CASE WHEN city_id = 10001 THEN 1 ELSE 0 END)) Counts number of people who live in NY
    # Count (*) counts total number of people belong to the gathering
    cursor.execute('''
        CREATE OR REPLACE VIEW 99_NY
        AS SELECT gathering_id
        FROM member_gathering, members
        WHERE member_gathering.member_id = members.id
        GROUP BY gathering_id
        HAVING (SUM(CASE WHEN city_id = 10001 THEN 1 ELSE 0 END)) >= (COUNT(*) * 0.99)
    ''')

    # Gatherings and members 99% NY
    cursor.execute('''
        CREATE VIEW maybe_NY_gatherings
        AS SELECT member_gathering.gathering_id AS gathering_id, member_id
        FROM 99_NY, member_gathering
        WHERE 99_NY.gathering_id = member_gathering.gathering_id
    ''')

    # Gatherings that have 100% of members live in NY
    cursor.execute('''
        CREATE OR REPLACE VIEW 100_NY
        AS SELECT gathering_id as gathering_id
        FROM maybe_NY_gatherings
        LEFT JOIN M ON maybe_NY_gatherings.member_id = M.id
        GROUP BY gathering_id
        HAVING ((SUM(CASE WHEN city_id = 10001 THEN 1 ELSE 0 END)) = (COUNT(*)))
    ''')

    # Gatherings and members of 100% NY
    cursor.execute('''
        CREATE VIEW NY_gatherings
        AS SELECT member_gathering.gathering_id AS gathering_id, member_id
        FROM 100_NY, member_gathering
        WHERE 100_NY.gathering_id = member_gathering.gathering_id
    ''')

    # -----------------------------------------------------------------

    # TODO: REQUIREMENT 5. MAKE HORIZONTAL DATAFRAME

    # Get non-duplicate (unique) values on Gathering ID
    cursor.execute('''
        SELECT DISTINCT gathering_id
        FROM NY_gatherings

    ''')

    gathering_id = cursor.fetchall()
    select_columns = ''

    # make select columns : eg max(IF(gathering_id = 10010442, 1, 0)
    # For more info, Check TA07 pg 13
    for dot in gathering_id:
        select_columns = select_columns + "max(IF(gathering_id='{0}',1,0)) as '{1}',".format(dot[0], dot[0])
    select_columns = select_columns[:-1]

    # Make Horizontal DataFrame
    cursor.execute('''
        SELECT member_id, ''' + select_columns + '''
        FROM NY_gatherings
        GROUP BY member_id 
    ''')

    # Save it as pandas
    df = pd.DataFrame(cursor.fetchall())
    df.columns = cursor.column_names
    df = df.set_index('member_id')
    print(df)

    cnx.close()
    cursor.close()
    # ----------------------------------------------

    # TODO: REQUIREMENT 6. SAVE ASSOCIATION RULES

    frequent_itemsets = apriori(df, min_support=0.0025, use_colnames=True)
    rules = association_rules(frequent_itemsets, metric='lift', min_threshold=0.5)
    rules.to_csv('DMA_project3_team10_association.txt')

    # ----------------------------------------------


if __name__ == '__main__':
    #part1()
    #part2()

