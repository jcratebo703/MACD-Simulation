val longestDays: Array[Int] = Array(30, 51, 50)

var count: Int = 0

for(x <- longestDays(0) to 5 by -1){
  for(y <- longestDays(1) to x + 5 by -1){
    for(z <- longestDays(2) to 5 by -1) {

    count+=1
    }
  }
}

print(count)