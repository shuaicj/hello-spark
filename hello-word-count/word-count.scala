val lines = sc.textFile("src/test/resources/words.txt")

val words = lines.flatMap(line => line.split(" "))

val mapCounts = words.map(word => (word, 1))

val counts = mapCounts.reduceByKey((a, b) => a + b)

counts.collect()
