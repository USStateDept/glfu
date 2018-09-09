import io
import pandas as pd
import requests
import re
from goose3 import Goose
from pandas.errors import EmptyDataError
#import zipfile


# file where each line is URL for data I've already saved
saved = open('data/saved.csv', 'r')
savedLines = saved.readlines()
saved.close()
toSave = open('data/saved.csv', 'a')

maindf = None
try:
    maindf = pd.read_csv('data/events.csv', sep=',')
except EmptyDataError:
    pass



#open('data/events.tsv', 'a')
#mentionFile = open('data/mentions.tsv', 'a')

fileListURL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt'

eventsColumnNames=pd.read_csv('~/Source/gdelt/eventColumnNames.tsv', sep='\t')
#mentionsColumnNames=pd.read_csv('~/Source/gdelt/mentionColumnNames.tsv', sep='\t')

eventTableCount = 0
#r = requests.get(fileListURL, stream=True)
r = requests.get(fileListURL)
iteratorOfDataURLs = reversed(r.text.split('\n'))
#for line in r.iter_lines():
#print('ok starting........')
for line in iteratorOfDataURLs:
    if (eventTableCount < 3 and line !=''):
   # if True: 
        #[anid, ahash, dataURL] = line.decode('UTF-8').split()
        [anid, ahash, dataURL] = line.split()
        #print(line.split())
        regex = r'.+gdeltv2/(\d{4})(\d\d)(\d\d)(\d{6})\.trans.+\.(\w+)\.\w{3}\.zip'
        matches = re.match(regex, dataURL)
        (year, month, day, time, tableMembership) = matches.groups()
        print(year, month, day, time, tableMembership)
        if dataURL in savedLines:
            print('dataURL was in saved.csv')
            pass
        elif (year!='2018' or month!='08'):
            print('wrong month')
            pass
        elif tableMembership=='export':
            #print('saving to saved.csv')
            toSave.write(dataURL+'\n')
            df = pd.read_csv(dataURL, compression='zip', sep='\t')
            if maindf is None:
                maindf = df
            else:
                maindf = pd.concat([maindf,df]) 

            eventTableCount += 1
            
        else:
            pass

toSave.close()
maindf.to_csv('data/events.csv', sep=',')


#eventFile.close()
#mentionFile.close()

##url = 'http://edition.cnn.com/2012/02/22/world/europe/uk-occupy-london/index.html?hpt=ieu_c2'
##g = Goose()
##article = g.extract(url=url)
##article.title
##article.meta_description
##article.cleaned_text[:150]
##article.top_image.src



        # write the URL to savedURL file and data to eventFile
#            if tableMembership!='gkg':
#                print('should save....')
#                r2=requests.get(dataURL)
#                contents = r2.text
#                print('saving to saved.csv')
#                toSave.write(dataURL+'\n')
#                if tableMembership=='export':
#                    pass#eventFile.write(contents)
#                else:
#                    pass#mentionFile.write(contents)
#
#def groupInto3s(mylist):
#    res = []
#    while len(mylist)>=3:
#        toAdd = mylist[:4]
#        mylist = mylist[4:]
#        res.append(toAdd)
#    return res
#
##fileListObj = requests.get(fileListURL)
##print(fileListObj.text[:50])
#
##events=pd.read_csv('~/Source/gdelt/data/events.tsv', sep=' ')
##events.columns=eventsColNames
##mentions=pd.read_csv('~/Source/gdelt/data/mentions.tsv', sep=' ')
##mentions.columns=mentions
#



            #reqForZip = requests.get(dataURL)
            #z = zipfile.ZipFile(io.BytesIO(reqForZip.content))
            #z.extractall()
            #csvString = z.open(z.namelist()[0]).read().decode('UTF-8'))
            #print(type(csv))
            #print(csv.read().decode('UTF-8')[:150])
