---
sidebar_position: 1
---

# Installation

## Requirements

- **Go 1.23** or later
- Linux, macOS, or Windows

## Install with Go Modules

```bash
go get github.com/chronicle-db/chronicle
```

## Verify Installation

Create a file called `main.go`:

```go
package main

import (
    "fmt"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    // Open a database
    db, err := chronicle.Open("mydata.db", chronicle.DefaultConfig("mydata.db"))
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Write a point
    err = db.Write(chronicle.Point{
        Metric:    "cpu_usage",
        Tags:      map[string]string{"host": "server1"},
        Value:     45.2,
        Timestamp: time.Now().UnixNano(),
    })
    if err != nil {
        panic(err)
    }

    fmt.Println("Chronicle is working!")
}
```

Run it:

```bash
go run main.go
# Output: Chronicle is working!
```

## Optional: Enable CGO

Chronicle works without CGO, but enabling it provides better compression performance:

```bash
CGO_ENABLED=1 go build
```

## What's Next?

- [Quick Start](/docs/getting-started/quick-start) - Build something in 5 minutes
- [Configuration](/docs/api-reference/configuration) - Tune Chronicle for your needs
- [Core Concepts](/docs/core-concepts/architecture) - Understand how Chronicle works
