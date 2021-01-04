''' Insert data into an sqlite database and query it. '''

import glob
import json
import os.path
import sqlite3

dataDir = 'data/'
jsonDir = os.path.join(dataDir, 'json')

write_to_disk = True # Set to False for fast testing
if write_to_disk:
    # Slow but persistent
    dbFile = os.path.join(dataDir, 'sqlite.db')
else:
    # Fast but transient
    dbFile = ':memory:'

    
def main():
    connection = sqlite3.connect(dbFile)
    cursor = connection.cursor()
    createSchema(cursor)
    populateSqlite(jsonDir, cursor)
    querySqlite(cursor)
    connection.commit()
    connection.close()


def createSchema(cursor, clearDb=True):
    ''' Create necessary tables in the sqlite database '''

    # drop all tables if requested
    if clearDb:
        cursor.execute('DROP TABLE IF EXISTS landmark_located_at_location;')
        cursor.execute('DROP TABLE IF EXISTS image_tagged_web_entity;')
        cursor.execute('DROP TABLE IF EXISTS image_contains_landmark;')
        cursor.execute('DROP TABLE IF EXISTS image_matches_image;')
        cursor.execute('DROP TABLE IF EXISTS image_in_page;')
        cursor.execute('DROP TABLE IF EXISTS image_tagged_label;')
        cursor.execute('DROP TABLE IF EXISTS web_entity;')
        cursor.execute('DROP TABLE IF EXISTS location;')
        cursor.execute('DROP TABLE IF EXISTS landmark;')
        cursor.execute('DROP TABLE IF EXISTS page;')
        cursor.execute('DROP TABLE IF EXISTS label;')
        cursor.execute('DROP TABLE IF EXISTS image;')

    # Create image table
    create_0 = '''
        CREATE TABLE image (
            id          INTEGER PRIMARY KEY,
            url         VARCHAR(256),
            is_document INTEGER(1) DEFAULT 0
        );
    '''
    cursor.execute(create_0)

    # Create label table
    create_1 = '''
        CREATE TABLE label (
            id          INTEGER PRIMARY KEY,
            mid         VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_1)

    # Create image_tagged_label table
    create_2 = '''
        CREATE TABLE image_tagged_label (
            id       INTEGER PRIMARY KEY,
            image_id INTEGER,
            label_id INTEGER,
            score    REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (label_id) REFERENCES label (id)
        );
    '''
    cursor.execute(create_2)

    # TODO: Create page table
    create_3 = '''
        CREATE TABLE page (
            id  INTEGER PRIMARY KEY,
            url VARCHAR(256)
        );
    '''
    cursor.execute(create_3)

    # TODO: Create landmark table
    create_4 = '''
        CREATE TABLE landmark (
            id          INTEGER PRIMARY KEY,
            mid         VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_4)

    # TODO: Create location table
    create_5 = '''
        CREATE TABLE location (
            id        INTEGER PRIMARY KEY,
            latitude  REAL,
            longitude REAL
        );
    '''
    cursor.execute(create_5)

    # TODO: Create web_entity table
    create_6 = '''
        CREATE TABLE web_entity (
            id          INTEGER PRIMARY KEY,
            entity_id   VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_6)

    # TODO: Create image_in_page table
    create_7 = '''
        CREATE TABLE image_in_page (
            id       INTEGER PRIMARY KEY,
            image_id INTEGER,
            page_id  INTEGER,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (page_id) REFERENCES page (id)
        );
    '''
    cursor.execute(create_7)

    # TODO: Create image_matches_image table
    create_8 = '''
        CREATE TABLE image_matches_image (
            id        INTEGER PRIMARY KEY,
            image_id1 INTEGER,
            image_id2 INTEGER,
            type      VARCHAR(8),
            FOREIGN KEY (image_id1) REFERENCES image (id),
            FOREIGN KEY (image_id2) REFERENCES image (id)
        );
    '''
    cursor.execute(create_8)

    # TODO: Create image_contains_landmark table
    create_9 = '''
        CREATE TABLE image_contains_landmark (
            id          INTEGER PRIMARY KEY,
            image_id    INTEGER,
            landmark_id INTEGER,
            score       REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (landmark_id) REFERENCES landmark (id)
        );
    '''
    cursor.execute(create_9)

    # TODO: Create image_tagged_web_entity table
    create_10 = '''
        CREATE TABLE image_tagged_web_entity (
            id            INTEGER PRIMARY KEY,
            image_id      INTEGER,
            web_entity_id INTEGER,
            score         REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (web_entity_id) REFERENCES web_entity (id)
        );
    '''
    cursor.execute(create_10)

    # TODO: Create landmark_located_at_location table
    create_11 = '''
        CREATE TABLE landmark_located_at_location (
            id          INTEGER PRIMARY KEY,
            landmark_id INTEGER,
            location_id INTEGER,
            FOREIGN KEY (landmark_id) REFERENCES landmark (id),
            FOREIGN KEY (location_id) REFERENCES location (id)
        );
    '''
    cursor.execute(create_11)


def populateSqlite(jsonDir, cursor):
    ''' Load Google JSON results into sqlite (schema must be created before) '''

    cnt = 0
    # Find and process all json files in the directory
    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('\n\nLoading', jsonFile, 'into sqlite')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
            insertImage(cursor, jsonData)
            cnt += 1

    print('\nLoaded', cnt, 'JSON documents into Sqlite\n')


def insertImage(cursor, jsonData):
    imageId = getOrCreateRow(cursor, 'image', 
        {'url': jsonData['url'], 'is_document': 1})
    print('Inserting Image With ID', imageId)

    # TODO: update isDocument attribute of image
    # done by changing parameter dictionary above

    # process labelAnnotations field
    for ann in jsonData['response']['labelAnnotations']:
        labelId = getOrCreateRow(cursor, 'label',
            {'mid': ann['mid'], 'description': ann['description']})
        ltiId = getOrCreateRow(cursor, 'image_tagged_label', 
            {'image_id': imageId, 'label_id': labelId, 'score': ann['score']})

    # TODO: process webDetection.fullMatchingImages field
    if 'fullMatchingImages' in jsonData['response']['webDetection']:
        for fmi in jsonData['response']['webDetection']['fullMatchingImages']:
            imageId2 = getOrCreateRow(cursor, 'image', {'url': fmi['url']})
            imiId = getOrCreateRow(cursor, 'image_matches_image', 
                {'image_id1': imageId, 'image_id2': imageId2, 'type': 'full'})
    
    # TODO: process webDetection.partialMatchingImages field
    if 'partialMatchingImages' in jsonData['response']['webDetection']:
        for pmi in \
        jsonData['response']['webDetection']['partialMatchingImages']:
            imageId2 = getOrCreateRow(cursor, 'image', {'url': pmi['url']})
            imiId = getOrCreateRow(cursor, 'image_matches_image', 
                {'image_id1': imageId, 'image_id2': imageId2, 
                 'type': 'partial'})
    
    # TODO: process webDetection.pagesWithMatchingImages field
    if 'pagesWithMatchingImages' in jsonData['response']['webDetection']:
        for pmi in \
        jsonData['response']['webDetection']['pagesWithMatchingImages']:
            pageId = getOrCreateRow(cursor, 'page', {'url': pmi['url']})
            iipId = getOrCreateRow(cursor, 'image_in_page', 
                {'image_id': imageId, 'page_id': pageId})
    
    # TODO: process webDetection.webEntities field 
    # (note: some webEntities have no description field)
    for ent in jsonData['response']['webDetection']['webEntities']:
        if 'description' in ent:
            desc = ent['description']
        else:
            desc = ''
        # shorter syntax: ent['description'] if 'description' in ent else ''
        webEntityId = getOrCreateRow(cursor, 'web_entity', 
            {'entity_id': ent['entityId'], 'description': desc})
        iteId = getOrCreateRow(cursor, 'image_tagged_web_entity', 
            {'image_id': imageId, 'web_entity_id': webEntityId, 
             'score': ent['score']})
    
    # TODO: process landmarkAnnotations and landmarkAnnotations.locations fields
    # (note: some landmarks have no description field)
    if 'landmarkAnnotations' in jsonData['response']:
        for lma in jsonData['response']['landmarkAnnotations']:
            if 'description' in lma:
                desc = lma['description']
            else:
                desc = ''
            landmarkId = getOrCreateRow(cursor, 'landmark', 
                {'mid': lma['mid'], 'description': desc})
            iclId = getOrCreateRow(cursor, 'image_contains_landmark', 
                {'image_id': imageId, 'landmark_id': landmarkId, 
                 'score': lma['score']})
            for loc in lma['locations']:
                locationId = getOrCreateRow(cursor, 'location', 
                    {'latitude': loc['latLng']['latitude'], 
                     'longitude': loc['latLng']['longitude']})
                lllId = getOrCreateRow(cursor, 'landmark_located_at_location', 
                    {'landmark_id': landmarkId, 'location_id': locationId})


def getOrCreateRow(cursor, table, dataDict):
    ''' Return the ID of a row of the given table with the given data.

    If the row does not already exists then create it first.  Existence is
    determined by matching on all supplied values.  Table is the table name,
    dataDict is a dict of {'attribute': value} pairs.
    '''

    whereClauses = ' AND '.join(['"{}" = :{}'.format(k, k) for k in dataDict])
    select = 'SELECT id FROM {} WHERE {}'.format(table, whereClauses)
    # print(select)

    cursor.execute(select, dataDict)
    res = cursor.fetchone()
    if res is not None:
        return res[0]

    fields = ','.join('"{}"'.format(k) for k in dataDict)
    values = ','.join(':{}'.format(k) for k in dataDict)
    insert = 'INSERT INTO {} ({}) values({})'.format(table, fields, values)
    # print(insert)
    cursor.execute(insert, dataDict)

    cursor.execute(select, dataDict)
    res = cursor.fetchone()
    if res is not None:
        return res[0]
    raise Exception('Something went wrong with ' + str(dataDict))


def querySqlite(cursor):
    ''' Run necessary queries and print results '''

    # 0. Count the total number of images in the database
    query_0 = '''
        SELECT COUNT(*) 
        FROM image;
    '''
    querySqliteAndPrintResults(query_0, cursor, title='Query 0')
    
    # TODO: 1. Count the total number of JSON documents in the database
    query_1 = '''
        SELECT COUNT(*)
        FROM   image
        WHERE  is_document == 1;
    '''
    querySqliteAndPrintResults(query_1, cursor, title='Query 1')

    # TODO: 2. Count the total number of Images, Labels, Landmarks,
    # Locations, Pages, and Web Entities in the database.
    query_2 = '''
        SELECT "image" entity, 
               COUNT(*) count
        FROM   image
        
        UNION

        SELECT "label" entity, 
               COUNT(*) count
        FROM   label
        
        UNION

        SELECT "landmark" entity, 
               COUNT(*) count
        FROM   landmark
        
        UNION

        SELECT "location" entity, 
               COUNT(*) count
        FROM   location
        
        UNION

        SELECT "page" entity, 
               COUNT(*) count
        FROM   page
        
        UNION

        SELECT "webEntity" entity, 
               COUNT(*) count
        FROM   web_entity;
    '''
    querySqliteAndPrintResults(query_2, cursor, title='Query 2')

    # TODO: 3. List the urls of all Images tagged with the Label with mid 
    # '/m/015kr' (which has the description 'bridge') ordered by the score of 
    # the association between them from highest to lowest
    query_3 = '''
        SELECT img.url, itl.score
        FROM   image img
        JOIN   image_tagged_label itl
        ON     img.id == itl.image_id
        JOIN   label lbl
        ON     itl.label_id == lbl.id
        WHERE  lbl.mid == "/m/015kr"
        ORDER BY itl.score DESC;
    '''
    querySqliteAndPrintResults(query_3, cursor, title='Query 3')

    # TODO: 4. List the 10 most frequent Web Entities that are associated with
    # the same Images as the Label with mid '/m/015kr' (which has the description 
    # 'bridge'). List them in descending order of the number of times they 
    # appear, followed by their entity_id alphabetically
    query_4 = '''
        SELECT ent.entity_id,
               ent.description, 
               COUNT(*)
        FROM   image_tagged_label itl
        JOIN   label lbl
        ON     itl.label_id == lbl.id
        JOIN   image_tagged_web_entity itw
        ON     itl.image_id == itw.image_id
        JOIN   web_entity ent
        ON     itw.web_entity_id == ent.id
        WHERE  lbl.mid == "/m/015kr"
        GROUP BY ent.id
        ORDER BY COUNT(*) DESC, ent.entity_id
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_4, cursor, title='Query 4')

    # TODO: 5. Find all Images associated with any Landmarks that are not 'New
    # York' or 'New York City' ordered alphabetically by landmark description 
    # and then by image URL.
    query_5 = '''
        SELECT lmk.description, img.url
        FROM   image img
        JOIN   image_contains_landmark icl
        ON     img.id == icl.image_id
        JOIN   landmark lmk
        ON     icl.landmark_id == lmk.id
        WHERE  lmk.description NOT IN ("New York", "New York City")
        ORDER BY lmk.description, img.url;
    '''
    querySqliteAndPrintResults(query_5, cursor, title='Query 5')

    # TODO: 6. List the 10 Labels that have been applied to the most
    # Images along with the number of Images each has been applied to
    query_6 = '''
        SELECT lbl.description, 
               COUNT(DISTINCT itl.image_id) img_cnt
        FROM   label lbl
        JOIN   image_tagged_label itl
        ON     lbl.id == itl.label_id
        GROUP BY lbl.description
        ORDER BY COUNT(DISTINCT itl.image_id) DESC
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_6, cursor, title='Query 6')

    # TODO: 7. List the 10 Pages that are linked to the most Images
    # through the webEntities.pagesWithMatchingImages JSON property
    # along with the number of Images linked to each one. Sort them by
    # count (descending) and then by page URL.
    query_7 = '''
        SELECT pag.url, 
               COUNT(DISTINCT iip.image_id) img_cnt
        FROM   page pag
        JOIN   image_in_page iip
        ON     pag.id == iip.page_id
        GROUP BY pag.id
        ORDER BY COUNT(DISTINCT iip.image_id) DESC,
                 pag.url
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_7, cursor, title='Query 7')

    # TODO: 8. List the 10 pairs of Images that appear on the most
    # Pages together through the webEntities.pagesWithMatchingImages
    # JSON property. List them in descending order of the number of
    # pages that they appear on together, then by the URL of the
    # first. Make sure that each pair is only listed once regardless
    # of which is first and which is second.
    query_8 = '''
        SELECT img1.url, img2.url, COUNT(DISTINCT iip1.page_id) pag_cnt
        FROM   image img1
        JOIN   image img2
        ON     img1.id < img2.id
        JOIN   image_in_page iip1
        ON     img1.id == iip1.image_id
        JOIN   image_in_page iip2
        ON     img2.id == iip2.image_id
        WHERE  iip1.page_id == iip2.page_id
        GROUP BY img1.id, img2.id
        ORDER BY COUNT(DISTINCT iip1.page_id) DESC
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_8, cursor, title='Query 8')


def querySqliteAndPrintResults(query, cursor, title='Running query:'):
    print()
    print(title)
    print(query)

    for record in cursor.execute(query):
        print(' ' * 4, end='')
        print('\t'.join([str(f) for f in record]))


if __name__ == '__main__':
    main()

        SELECT imgTagLab.image_id ,count( imgTagLab.image_id) cnt
        FROM image_tagged_label imgTagLab
        
        UNION ALL
        
        SELECT image_in_page.image_id ,count( image_in_page.image_id) cnt
        FROM image_in_page
        
        UNION ALL
        
        SELECT imagMatch.image_id1, count(imagMatch.image_id1) cnt
        FROM image_matches_image imagMatch
        
        UNION ALL
        
        SELECT imagMatch.image_id2,count(imagMatch.image_id2) cnt
        FROM image_matches_image imagMatch
        
        UNION ALL
        
        SELECT imgland.image_id,count( imgland.image_id) cnt
        FROM image_contains_landmark imgland
        
        UNION ALL
        
        SELECT imgWeb.image_id,count( imgWeb.image_id) cnt
        FROM image_tagged_web_entity imgWeb
        
        UNION ALL
        
        SELECT imgWeb.image_id count( imgWeb.image_id) cnt
        FROM image_tagged_web_entity imgWeb
    
