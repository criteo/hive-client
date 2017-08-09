# A Scala Hive Client

An *experimental* asynchronous pure Thrift Hive client.

## Why?
```
> dependency-tree
[info] Resolving jline#jline;2.14.3 ...
[info] Done updating.
[info] org.criteo:hive-client_2.11:0.1.0-SNAPSHOT [S]
[info]   +-org.apache.thrift:libthrift:0.10.0
[info]     +-org.apache.httpcomponents:httpclient:4.4.1
[info]     | +-commons-codec:commons-codec:1.9
[info]     | +-commons-logging:commons-logging:1.2
[info]     | +-org.apache.httpcomponents:httpcore:4.4.1
[info]     | 
[info]     +-org.apache.httpcomponents:httpcore:4.4.1
[info]     +-org.slf4j:slf4j-api:1.7.12
[info]     
[success] Total time: 1 s, completed Jul 9, 2017 3:27:26 PM
```

If you'd like to embed Hive access into a Scala application, you should be very happy to see the above dep tree.
