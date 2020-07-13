## Target

Low-memory applications: queue doesn't cache anything except counters.

## Performance

    BenchmarkQueue_Put-8          	   53086	     20844 ns/op
    BenchmarkQueue_PeekCommit-8   	   83653	     13663 ns/op

i7-8550U CPU, SSD, Ubuntu 18.04 x64