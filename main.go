package main

import (
    "database/sql"
    "fmt"
    "os"
    "time"
    "sync"
    _ "github.com/go-sql-driver/mysql"
)

func main(){
    fmt.Printf("Hello world. \n")
    pid := os.Getpid()

    var wg sync.WaitGroup
    wg.Add(2)
    go DBInsert(pid+1, &wg)
    go DBInsert(pid+2, &wg)

    fmt.Println("Waiting for goroutines to finish... ")
    wg.Wait()
    fmt.Println("Done! ")

}

func DBInsert(pid int, wg *sync.WaitGroup) {
    defer wg.Done()

    db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    // db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
    if err != nil {
        panic(err)
    }

    err = db.Ping()
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    db.SetConnMaxLifetime(time.Minute * 3)
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(10)

    stmtIns, err := db.Prepare("INSERT INTO squareNum VALUES( ?, ?, ? )") // ? = placeholder
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
    defer stmtIns.Close() // Close the statement when we leave main() / the program terminates

    // Prepare statement for reading data
    stmtOut, err := db.Prepare("SELECT squareNumber FROM squarenum WHERE number = ?")
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
    defer stmtOut.Close()

    // Insert square numbers for 0-24 in the database
    InsertData(stmtIns, pid, 30000 )
//    for i := 0; i < 20000; i++ {
//        _, err = stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
//        if err != nil {
//            panic(err.Error()) // proper error handling instead of panic in your app
//        }
//    }

    var squareNum int // we "scan" the result in here

    // Query the square-number of 13
    err = stmtOut.QueryRow(13).Scan(&squareNum) // WHERE number = 13
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
    fmt.Printf("The square number of 13 is: %d \n", squareNum)

    // Query another number.. 1 maybe?
    err = stmtOut.QueryRow(1).Scan(&squareNum) // WHERE number = 1
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    fmt.Printf("The square number of 1 is: %d \n", squareNum)
}

func InsertData(stmtIns *sql.Stmt, pid int, loop int) {
    defer func() {
        fmt.Printf("Failed to insert the data \n")
        if r := recover(); r != nil {
//            fmt.Println("Recovered in f", r)
            fmt.Printf("Failed to run the query in the InsertData \n\n\n")
        }
    }()
    for i := 0; i < loop; i++ {
        _, err := stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
        if err != nil {
            panic(err.Error()) // proper error handling instead of panic in your app
        }
    }
}
