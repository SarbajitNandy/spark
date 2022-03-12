""" Slash handler
"""
def handlerForSlashAdded(df,delimiter,outfilepath,spark):
    print("handle slash function")
    print(outfilepath)
    dfOut = df.rdd.map(lambda row : getSlashAdded(row, delimiter))
    dfOut.saveastextfile(outfilepath)
    dfOut = spark.read.format('text').load(outfilepath)
    return dfOut

def getSlashAdded(row, delimter):
    rowList = []
    for col in row.split(delimter):
        slist = str(col)

        for id in range(len(col)):
            if slist[id]=='"' and 1<id<len(col)-1:
                slist[id]='\\"'
            if slist[id]=='\\' and len(col)-3<=id<len(col)-1:
                slist[id]='\\\\'
        newCol = ''.join(slist)
        rowList.append(newCol)
    newValue = delimter.join(rowList)
    return newValue