package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "context"
    "os"
    "time"
    "sync"
    "regexp"
    "os/exec"

    _ "github.com/go-sql-driver/mysql"
    "github.com/luyomo/tisample/pkg/tui"
    clientv3 "go.etcd.io/etcd/client/v3"
)


var LK = sync.RWMutex{}

type Component struct {
    IP string `json:"ip"`
}


func main(){
    fmt.Printf("Hello world. \n")
    pid := os.Getpid()

    // RestartTiDBCluster()

    ctx, cancel := context.WithCancel(context.Background())

    retry := 0
    var wg sync.WaitGroup
    wg.Add(2)
    go DBInsert(pid+1, &wg, &retry)
    go DBInsert(pid+2, &wg, &retry)
    go RestartTiDBCluster(ctx)


    fmt.Println("Waiting for goroutines to finish... ")
    wg.Wait()
    fmt.Printf("Starting to cancel all the processes \n\n\n")
    cancel()
    fmt.Printf("Retried : <%d>\n", retry)
    fmt.Println("Done! ")

    tableOutput := [][]string{{"Expected Insert Row", "Actual Insert Row", "Execution Time", "# of Retry", "# of threads", "# of TiDB Restart"}}
    tableOutput = append(tableOutput, []string{"200000", "200000", "20 second", "3", "2", "3"} )
    tui.PrintTable(tableOutput, true)
}

func RestartTiDBCluster(ctx context.Context) {
    var tidbIPs []string
    client, err := clientv3.New(clientv3.Config{Endpoints:   []string{"182.83.1.86:2379"}  })
    if err != nil {
        panic(err)
    }

//    ctx, cancel := context.WithTimeout(client.Ctx(), 10 * time.Second)
//    defer cancel()
    members , err := client.MemberList(ctx)
    if err != nil {
        panic(err)
    }
    fmt.Printf("The members are <%#v> \n", members)

    kv := clientv3.NewKV(client)
    gr, _ := kv.Get(ctx, "/topology/tidb", clientv3.WithPrefix())
    for _, tidbNode := range gr.Kvs {
        match, _ := regexp.MatchString("/info$",string( tidbNode.Key) ) 
	if match == true {
            jsonData, err := componentFromJSON(string(tidbNode.Value) )
            if err != nil {
                panic(err)
            }
            fmt.Println("Value: ", string(tidbNode.Value), "Revision: ", string(tidbNode.Key))
            fmt.Printf("The IP address is <%s> \n", jsonData.IP)
            tidbIPs = append(tidbIPs, jsonData.IP)
        }else{
            fmt.Println("Unmatched -> Value: ", string(tidbNode.Value), "Revision: ", string(tidbNode.Key))
        }
    }

    fmt.Printf("The IP is <%#v> \n", tidbIPs)
    ticker := time.NewTicker(time.Duration(15) * time.Second)

    for {
         select {
             case <-ticker.C:
                 fmt.Printf("Starting to restart TiDB Node \n\n\n")
                 tidbIPs = append(tidbIPs[1:], tidbIPs[0])

                 cmd := exec.Command("/home/admin/.tiup/bin/tiup", "cluster", "restart", "mgtest", "-y", "--node", fmt.Sprintf("%s:4000", tidbIPs[0]))
                 err := cmd.Run()
                 if err != nil {
                     fmt.Printf("The error is <%s> \n", err.Error())
                 }
             case <-ctx.Done():
                 fmt.Printf("Starting to stop all the instances \n\n\n")
                 return
         }
 
     }
}

func componentFromJSON(str string) (s Component, err error) {
    if err = json.Unmarshal([]byte(str), &s); err != nil {
        panic(err)
    }
    return
}

func PrepareDBConn() (error, *sql.DB, *sql.Stmt, *sql.Stmt) {
    //db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
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

    // Prepare statement for reading data
    stmtOut, err := db.Prepare("SELECT squareNumber FROM squarenum WHERE number = ?")
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
//    defer stmtOut.Close()

    return nil, db, stmtIns, stmtOut 
}

func DBInsert(pid int, wg *sync.WaitGroup, _retry *int) {
    defer wg.Done()

    err, _, stmtIns, stmtOut := PrepareDBConn()

    // Insert square numbers for 0-24 in the database
    InsertData(stmtIns, stmtOut, pid, _retry,  100000)

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

func InsertData(stmtIns *sql.Stmt, stmtOut *sql.Stmt, pid int, _pRetry *int,  loop int) {
//    defer func() {
//        fmt.Printf("Failed to insert the data \n")
//        if r := recover(); r != nil {
////            fmt.Println("Recovered in f", r)
//            fmt.Printf("Failed to run the query in the InsertData \n\n\n")
//        }
//    }()
    for i := 0; i < loop; i++ {
        _, err := stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
        if err != nil {
            if err.Error() == "invalid connection"{
                err, _, stmtIns, stmtOut = PrepareDBConn()
		if err != nil {
                    panic(err)
	        }
		i = i - 1
		LK.Lock()
		*_pRetry = *_pRetry + 1
		LK.Unlock()
	    } else{
                fmt.Printf("The error is <%s>\n\n\n", err.Error())
                panic(err.Error()) // proper error handling instead of panic in your app
	    }
        }
    }
}
