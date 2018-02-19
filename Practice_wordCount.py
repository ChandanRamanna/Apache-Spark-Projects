import codecs


file = codecs.open("/Users/chandanramanna/Desktop/Spark/Book.txt", "r", encoding="utf-8-sig", errors = "ignore")
data = file.read()
wordcount = {}

for word in data.split():
    if (word not in wordcount):
        wordcount[word] = 1
    else:
        wordcount[word] += 1

file.close()

for key, value in wordcount.items():
    print(key, value)

