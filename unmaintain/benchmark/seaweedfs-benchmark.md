## SeaweedFS Benchmark

阿里云 200G 高效云盘，大约 3000 IOPS。

### Write

```
Concurrency Level:      16
Time taken for tests:   384.116 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106812710 bytes
Requests per second:    2729.84 [#/sec]
Transfer rate:          2813.92 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.5      5.8       107.9      3.5

Percentage of the requests served within a certain time (ms)
   50%      4.8 ms
   66%      6.4 ms
   75%      7.2 ms
   80%      7.9 ms
   90%     10.0 ms
   95%     12.4 ms
   98%     15.5 ms
   99%     18.2 ms
  100%    107.9 ms
```

### Read

```
Concurrency Level:      16
Time taken for tests:   149.622 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106776080 bytes
Requests per second:    7008.19 [#/sec]
Transfer rate:          7223.80 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.1      2.2       395.3      2.2

Percentage of the requests served within a certain time (ms)
   50%      1.7 ms
   66%      2.4 ms
   75%      2.9 ms
   80%      3.1 ms
   90%      3.6 ms
   95%      5.0 ms
   98%      7.6 ms
   99%      9.7 ms
  100%    395.3 ms
```
