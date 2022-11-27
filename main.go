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
    "strconv"

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

    PreProcess()

    ctx, cancel := context.WithCancel(context.Background())

    threads := 2
    retry := 0
    numRestart := 0
    numRows := 60000
    insertRow := 0
    var wg sync.WaitGroup
    _startTime := time.Now()
    wg.Add(threads)
    for idx:=0; idx< threads; idx++{
      go DBInsert(pid+idx+1, numRows, &wg, &retry)
    }
    go RestartTiDBCluster(ctx, &numRestart)


    fmt.Println("Waiting for goroutines to finish... ")
    wg.Wait()
    fmt.Printf("Starting to cancel all the processes \n\n\n")
    cancel()
    fmt.Printf("Retried : <%d>\n", retry)
    fmt.Println("Done! ")
    _elapsed := time.Since(_startTime)

    PostProcess(&insertRow)
    strDuration := fmt.Sprintf("%v", _elapsed)
    fmt.Printf("The elapsed time is <%s> \n", strDuration)

    tableOutput := [][]string{{"Expected Insert Row", "Actual Insert Row", "Execution Time", "# of Retry", "# of threads", "# of TiDB Restart"}}
    tableOutput = append(tableOutput, []string{strconv.Itoa(numRows * threads), strconv.Itoa(insertRow), strDuration , strconv.Itoa(retry) , strconv.Itoa(threads), strconv.Itoa(numRestart)} )
    tui.PrintTable(tableOutput, true)
}

func RestartTiDBCluster(ctx context.Context, numRestart *int) {
    var tidbIPs []string
    client, err := clientv3.New(clientv3.Config{Endpoints:   []string{"182.83.1.86:2379"}  })
    if err != nil {
        panic(err)
    }

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
                 tidbIPs = append(tidbIPs[1:], tidbIPs[0])

                 fmt.Printf("Starting to restart TiDB Node<%s> \n", tidbIPs[0])
                 cmd := exec.Command("/home/admin/.tiup/bin/tiup", "cluster", "restart", "mgtest", "-y", "--node", fmt.Sprintf("%s:4000", tidbIPs[0]))
                 err := cmd.Run()
                 if err != nil {
                     fmt.Printf("The error is <%s> \n", err.Error())
		     panic(err)
                 }
		 *numRestart = *numRestart + 1
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

func PreProcess() {
    db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    //db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    db.SetConnMaxLifetime(time.Minute * 3)
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(10)

    _, err = db.Exec("truncate table test.tiproxy_test") // ? = placeholder
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
}

func PostProcess(insertRow *int) {
    //db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    db.SetConnMaxLifetime(time.Minute * 3)
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(10)

    stmtOut, err := db.Prepare("SELECT count(*) FROM tiproxy_test")
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    err = stmtOut.QueryRow().Scan(insertRow) // WHERE number = 1
    if err != nil {
        if err.Error() == "invalid connection"{
            fmt.Printf("Starting to re-connect the DB \n\n\n")
            err, stmtOut = PrepareDBConn()
            if err != nil  {
                panic(err)
            }

            err = stmtOut.QueryRow(1).Scan(insertRow) // WHERE number = 1
        } else{
            panic(err.Error()) // proper error handling instead of panic in your app
        }
    }
}

func PrepareDBConn() (error, *sql.Stmt) {
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

    stmtIns, err := db.Prepare("INSERT INTO tiproxy_test VALUES( ?, ?, ? )") // ? = placeholder
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    return nil, stmtIns 
}

func DBInsert(pid, numRows int, wg *sync.WaitGroup, _retry *int) {
    defer wg.Done()

    err, stmtIns := PrepareDBConn()
    if err != nil {
        panic(err)
    }

    // Insert square numbers for 0-24 in the database

    for i := 0; i < numRows; i++ {
        _, err := stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
        if err != nil {
            if err.Error() == "invalid connection"{
                fmt.Printf("Starting to re-connect the DB \n\n\n")
                err, stmtIns = PrepareDBConn()
		if err != nil {
                    panic(err)
	        }
		i = i - 1
		LK.Lock()
		*_retry = *_retry + 1
		LK.Unlock()
	    } else{
                fmt.Printf("The error is <%s>\n\n\n", err.Error())
                panic(err.Error()) // proper error handling instead of panic in your app
	    }
        }
    }

}
