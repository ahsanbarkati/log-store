# Log Store
[<img src="log-store.png" width="300"/>](log-store.png)

Log store is a persistent record storage system. It can be used inscenarios where records only needs to be appended and replayed, for example in a Write-Ahead Log in distributed systems. This implementation is thread safe and enables fast append, truncate and replay operations.

The major APIs are:
- `OpenLogStore(dir string)` - Open/Create a log store at the given directory.
- `Append(data []byte)` - Appends the data as a record in the log store.
- `GetPosition()` - Returns the current position of the log store.
- `Truncate(idx uint64)` - Truncates the records before the given index.
- `Replay(idx uint64, callback func)` - Replays the log store store starting from the given index.
- `Close()` - Close the log store.
