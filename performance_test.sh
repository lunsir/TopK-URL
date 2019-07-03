#! /bin/bash
#this script will generate a 100G file, and use it to  test the performance.
for i in {1..24}; do
	cp input.txt  input.txt1
    cat input.txt1 >> input.txt
done	
unlink input.txt1
go build main.go
time ./main
rm -f input.text-*
git checkout input.txt
