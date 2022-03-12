from settings import *
from Utilities.Preprocessorutil1 import handlerForSlashAdded

# spark
df = spark.read.format('csv').load(os.path.join(DATA_DIR, "test.csv"))
print("test script")
dfOut= handlerForSlashAdded(df, '|', spark)
print("after run ")
print(dfOut.show())
