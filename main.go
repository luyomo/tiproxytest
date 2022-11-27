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
    "flag"
    "github.com/fatih/color"

    _ "github.com/go-sql-driver/mysql"
    "github.com/luyomo/tisample/pkg/tui"
    clientv3 "go.etcd.io/etcd/client/v3"
)


var LK = sync.RWMutex{}

type Component struct {
    IP string `json:"ip"`
}

type Arguments struct {
    DBUser string
    DBHost string
    DBPort int
    DBPassword string
    DBName string
    PDHost string
    PDPort int
    Threads int
    Rows int
    Interval int
    Message string
}

func main(){
    var arguments Arguments
    flag.StringVar(&arguments.DBUser, "db-user", "root", "db connection user. default: root")
    flag.StringVar(&arguments.DBHost, "db-host", "127.0.0.1", "db connection host. default: 127.0.0.1")
    flag.IntVar(&arguments.DBPort, "db-port", 6000, "db connection port. default: 6000")
    flag.StringVar(&arguments.DBPassword, "db-password", "", "db connection password. default:")
    flag.StringVar(&arguments.DBName, "db-name", "test", "db name. default: test")
    flag.StringVar(&arguments.PDHost, "pd-host", "", "pd host")
    flag.IntVar(&arguments.PDPort, "pd-port", 2379, "pd port")
    flag.IntVar(&arguments.Threads, "threads", 2, "# of threads")
    flag.IntVar(&arguments.Rows, "rows", 60000, "rows per thread. default: 10000")
    flag.IntVar(&arguments.Interval, "interval", 10, "sleep time. default: 10s")
    flag.StringVar(&arguments.Message, "message", "no explanation", "Message to explain the test")


    flag.Parse()
    tableArgs := [][]string{{"Parameter Name", "Parameter Value", "Comment" }}
    tableArgs = append(tableArgs, []string{"Connection DB Host", arguments.DBHost, "As parameter name" })
    tableArgs = append(tableArgs, []string{"Connection DB Port", strconv.Itoa(arguments.DBPort), "As parameter name" })
    tableArgs = append(tableArgs, []string{"Connection DB User", arguments.DBUser, "As parameter name" })
    tableArgs = append(tableArgs, []string{"Connection DB Name", arguments.DBName, "As parameter name" })
    tableArgs = append(tableArgs, []string{"PD Host", arguments.PDHost, "PD Host to fetch the TiDB Node" })
    tableArgs = append(tableArgs, []string{"PD Port", strconv.Itoa(arguments.PDPort), "PD Port to fetch the TiDB Node" })
    tableArgs = append(tableArgs, []string{"Number of threads", strconv.Itoa(arguments.Threads), "Number of threads to insert data" })
    tableArgs = append(tableArgs, []string{"Number of records per thread", strconv.Itoa(arguments.Rows), "Number of rows per thread to insert data" })
    tableArgs = append(tableArgs, []string{"Interval to restart TiDB(seconds)", strconv.Itoa(arguments.Interval), "Restart TiDB node at the interval" })
    tui.PrintTable(tableArgs, true)

    c := color.New(color.FgCyan).Add(color.Underline).Add(color.Bold)
    c.Println(fmt.Sprintf("\n\n          %s           \n\n", arguments.Message) )

//    fmt.Printf("\n\n")
//    color.Cyan(fmt.Sprintf("\n\n          %s \n\n", arguments.Message) )
    // fmt.Printf("The db user is <%s> \n", arguments.DBUser)
    // fmt.Printf("The db host is <%s> \n", arguments.DBHost)
    // fmt.Printf("The db port is <%d> \n", arguments.DBPort)
    // fmt.Printf("The db password is <%s> \n", arguments.DBPassword)
    // fmt.Printf("The pd host is <%s> \n", arguments.PDHost)
    // fmt.Printf("The pd port is <%d> \n", arguments.PDPort)
    // fmt.Printf("The thread is <%d> \n", arguments.Threads)
    // fmt.Printf("The rows is <%d> \n", arguments.Rows)
    // fmt.Printf("The interval is <%d> \n", arguments.Interval)

    pid := os.Getpid()

    PreProcess(&arguments)

    ctx, cancel := context.WithCancel(context.Background())

    threads := 2
    retry := 0
    numRestart := 0
    insertRow := 0
    var wg sync.WaitGroup
    _startTime := time.Now()
    wg.Add(threads)
    for idx:=0; idx< threads; idx++{
      go DBInsert(&arguments, pid+idx+1, &wg, &retry)
    }
    go RestartTiDBCluster(ctx, &arguments, &numRestart)


    //fmt.Println("Waiting for goroutines to finish... ")
    wg.Wait()
    //fmt.Printf("Starting to cancel all the processes \n\n\n")
    cancel()
    //fmt.Printf("Retried : <%d>\n", retry)
    //fmt.Println("Done! ")
    _elapsed := time.Since(_startTime)

    PostProcess(&arguments, &insertRow)
    strDuration := fmt.Sprintf("%v", _elapsed)
    //fmt.Printf("The elapsed time is <%s> \n", strDuration)

    tableOutput := [][]string{{"Expected Insert Row", "Actual Insert Row", "Execution Time", "# of Retry", "# of threads", "# of TiDB Restart"}}
    tableOutput = append(tableOutput, []string{strconv.Itoa(arguments.Rows * threads), strconv.Itoa(insertRow), strDuration , strconv.Itoa(retry) , strconv.Itoa(threads), strconv.Itoa(numRestart)} )
    tui.PrintTable(tableOutput, true)
}

func RestartTiDBCluster(ctx context.Context, args *Arguments, numRestart *int) {
    if (*args).Interval == 0 {
        return
    }
    var tidbIPs []string
    client, err := clientv3.New(clientv3.Config{Endpoints:   []string{fmt.Sprintf("%s:%d", (*args).PDHost, (*args).PDPort) }  })
    if err != nil {
        panic(err)
    }

    //members , err := client.MemberList(ctx)
    //if err != nil {
    //    panic(err)
    //}
    //fmt.Printf("The members are <%#v> \n", members)

    kv := clientv3.NewKV(client)
    gr, _ := kv.Get(ctx, "/topology/tidb", clientv3.WithPrefix())
    for _, tidbNode := range gr.Kvs {
        match, _ := regexp.MatchString("/info$",string( tidbNode.Key) ) 
	if match == true {
            jsonData, err := componentFromJSON(string(tidbNode.Value) )
            if err != nil {
                panic(err)
            }
            //fmt.Println("Value: ", string(tidbNode.Value), "Revision: ", string(tidbNode.Key))
            //fmt.Printf("The IP address is <%s> \n", jsonData.IP)
            tidbIPs = append(tidbIPs, jsonData.IP)
        }else{
            //fmt.Println("Unmatched -> Value: ", string(tidbNode.Value), "Revision: ", string(tidbNode.Key))
        }
    }

    //fmt.Printf("The IP is <%#v> \n", tidbIPs)
    ticker := time.NewTicker(time.Duration((*args).Interval) * time.Second)

    for {
         select {
             case <-ticker.C:
                 tidbIPs = append(tidbIPs[1:], tidbIPs[0])

                 fmt.Printf("Restart TiDB Node<%s> \n", tidbIPs[0])
                 cmd := exec.Command("/home/admin/.tiup/bin/tiup", "cluster", "restart", "mgtest", "-y", "--node", fmt.Sprintf("%s:4000", tidbIPs[0]))
                 err := cmd.Run()
                 if err != nil {
                     //fmt.Printf("The error is <%s> \n", err.Error())
		     panic(err)
                 }
		 *numRestart = *numRestart + 1
             case <-ctx.Done():
                 //fmt.Printf("Starting to stop all the instances \n\n\n")
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

func PreProcess(args *Arguments) {
    //db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    //db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*args).DBUser, (*args).DBPassword, (*args).DBHost, (*args).DBPort, (*args).DBName))
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

func PostProcess(args *Arguments, insertRow *int) {
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*args).DBUser, (*args).DBPassword, (*args).DBHost, (*args).DBPort, (*args).DBName))
    //db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    //db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
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
            //fmt.Printf("Starting to re-connect the DB \n\n\n")
            err, stmtOut = PrepareDBConn(args)
            if err != nil  {
                panic(err)
            }

            err = stmtOut.QueryRow(1).Scan(insertRow) // WHERE number = 1
        } else{
            panic(err.Error()) // proper error handling instead of panic in your app
        }
    }
}

func PrepareDBConn(args *Arguments) (error, *sql.Stmt) {
    //db, err := sql.Open("mysql", "root:@tcp(mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com:4000)/test")
    //db, err := sql.Open("mysql", "root:@tcp(172.82.11.164:6000)/test")
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*args).DBUser, (*args).DBPassword, (*args).DBHost, (*args).DBPort, (*args).DBName))
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

func DBInsert(args *Arguments, pid int, wg *sync.WaitGroup, _retry *int) {
    defer wg.Done()

    err, stmtIns := PrepareDBConn(args)
    if err != nil {
        panic(err)
    }

    // Insert square numbers for 0-24 in the database

    for i := 0; i < (*args).Rows ; i++ {
        _, err := stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
        if err != nil {
            if err.Error() == "invalid connection"{
                fmt.Printf("Starting to re-connect the DB \n\n\n")
                err, stmtIns = PrepareDBConn(args)
		if err != nil {
                    panic(err)
	        }
		i = i - 1
		LK.Lock()
		*_retry = *_retry + 1
		LK.Unlock()
	    } else{
                //fmt.Printf("The error is <%s>\n\n\n", err.Error())
                panic(err.Error()) // proper error handling instead of panic in your app
	    }
        }
    }

}
