# TopK-URL
TopK-URL is a tool that can quickly find the top k most frequent lines(e.g. url) in a file with limited memory(or find the most unfrequent lines with a small change to the code).
## Implementation
The main algorithm used in this program is hash aggregate and priority queue(or heap sort). if the file is small enough to load in memory it will be directly loaded to build a hashmap and push all the hashmap items into a priority queue with limited size, then we get the result we want. otherwise we sharding the big file into some small files(can be loaded in memory) by the hash value of the line(this method can promise all identical lines ard divied into a single file). For every small file we can get the top k most frequent lines within the file with the algorithm described above. then merge them into a single priority queue.
## Test and Build
To test and build this project into a executable file you should have golang intalled on your machine. Here is a quick command to install golang on debian or ubuntu.
```
$sudo apt-get install golang-go
```
You can run the unit test with the following command:
```
$go test
```
You can build this project with the following command:
```
$go build main.go
```
then you can get a executable file named main.

## Usage
After the build, you can run the executable file main, then it will print the most frequent lines of input.txt which is in the same directory. You can replace it as needed. But you should always keep input.txt and the executable file in the same directory.

```
$ ./main
```
## Performance
You can test the performance of this program with the following command. To run this test you should have at least 200G space left on your disk.
```
$./performance_test.sh
```
