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
    "github.com/luyomo/tisample/pkg/tui/progress"

    clientv3 "go.etcd.io/etcd/client/v3"
)


var LK = sync.RWMutex{}

type Component struct {
    IP string `json:"ip"`
}

type Arguments struct {
    DBUser     string
    DBHost     string
    DBPort     int
    DBPassword string
    DBName     string

    PDHost     string
    PDPort     int

    Threads    int
    Rows       int
    Interval   int
    Message    string
}

type SessionInfo struct {
    ID       int
    Hostname string
}

type TiProxyTest struct {
    Args             *Arguments
    InsertedRow      int `default:0`
    NumOfTiDBRestart int `default:0`
    NumOfRetry       int `default:0`
    LstSessionInfo   []*SessionInfo
}

func main(){
    var insTiProxyTest TiProxyTest
    insTiProxyTest.Args = initArgs()

    insTiProxyTest.PreProcess()

    _startTime := time.Now()
    insTiProxyTest.Execute()
    _elapsed := time.Since(_startTime)

    insTiProxyTest.PostProcess()

    strDuration := fmt.Sprintf("%v", _elapsed)

    tableOutput := [][]string{{"Expected Insert Row", "Actual Insert Row", "Execution Time", "# of Retry", "# of threads", "# of TiDB Restart"}}
    tableOutput = append(tableOutput, []string{strconv.Itoa((*insTiProxyTest.Args).Rows * (*insTiProxyTest.Args).Threads), strconv.Itoa(insTiProxyTest.InsertedRow), strDuration , strconv.Itoa(insTiProxyTest.NumOfRetry) , strconv.Itoa((*insTiProxyTest.Args).Threads), strconv.Itoa(insTiProxyTest.NumOfTiDBRestart)} )
    tui.PrintTable(tableOutput, true)

    return
}

func initArgs() *Arguments {
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

    return &arguments
}

func (t *TiProxyTest) Execute() {
    c := color.New(color.FgCyan).Add(color.Underline).Add(color.Bold)
    c.Println(fmt.Sprintf("\n\n          %s           \n\n", (*t.Args).Message) )

    displayMsg := "Data insertion/Per thread: %d/" + strconv.Itoa((*t.Args).Rows) + ", Number of TiDB restart: %d, Client re-connect: %d"

    var progressBar progress.Bar
    progressBar = progress.NewSingleBar(fmt.Sprintf(displayMsg, 0, 0, 0))
    if singleBar, ok := progressBar.(*progress.SingleBar); ok {
        singleBar.StartRenderLoop()
    } 

    pid := os.Getpid()

    ctx, cancel := context.WithCancel(context.Background())

    var wg sync.WaitGroup
    wg.Add((*t.Args).Threads)
    for idx:=0; idx < (*t.Args).Threads; idx++{
      go t.DBInsert(idx, pid+idx+1, &wg, &progressBar, &displayMsg)
    }
    go t.RestartTiDBCluster(ctx, &progressBar, &displayMsg)

    wg.Wait()
    cancel()
    if singleBar, ok := progressBar.(*progress.SingleBar); ok {
        singleBar.StopRenderLoop()
    }  
}

func (t *TiProxyTest)RestartTiDBCluster(ctx context.Context, progressBar *progress.Bar, displayMsg *string) {
    if (*t.Args).Interval == 0 {
        return
    }
    var tidbIPs []string
    client, err := clientv3.New(clientv3.Config{Endpoints:   []string{fmt.Sprintf("%s:%d", (*t.Args).PDHost, (*t.Args).PDPort) }  })
    if err != nil {
        panic(err)
    }

    kv := clientv3.NewKV(client)
    gr, _ := kv.Get(ctx, "/topology/tidb", clientv3.WithPrefix())
    for _, tidbNode := range gr.Kvs {
        match, _ := regexp.MatchString("/info$",string( tidbNode.Key) ) 
	if match == true {
            jsonData, err := componentFromJSON(string(tidbNode.Value) )
            if err != nil {
                panic(err)
            }
            tidbIPs = append(tidbIPs, jsonData.IP)
        }
    }

    ticker := time.NewTicker(time.Duration((*t.Args).Interval) * time.Second)

    for {
         select {
             case <-ticker.C:
                 tidbIPs = append(tidbIPs[1:], tidbIPs[0])

                 cmd := exec.Command("/home/admin/.tiup/bin/tiup", "cluster", "restart", "mgtest", "-y", "--node", fmt.Sprintf("%s:4000", tidbIPs[0]))
                 err := cmd.Run()
                 if err != nil {
		     panic(err)
                 }
                 t.NumOfTiDBRestart = t.NumOfTiDBRestart + 1
                 (*progressBar).UpdateDisplay(&progress.DisplayProps{
                     Prefix: fmt.Sprintf(*displayMsg, t.InsertedRow, t.NumOfTiDBRestart, t.NumOfRetry),
                 }) 
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

func (t *TiProxyTest)PreProcess() {
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*t.Args).DBUser, (*t.Args).DBPassword, (*t.Args).DBHost, (*t.Args).DBPort, (*t.Args).DBName))
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

func (t *TiProxyTest)PostProcess() {
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*t.Args).DBUser, (*t.Args).DBPassword, (*t.Args).DBHost, (*t.Args).DBPort, (*t.Args).DBName))
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

    err = stmtOut.QueryRow().Scan(&t.InsertedRow) // WHERE number = 1
    if err != nil {
        if err.Error() == "invalid connection"{
            err, stmtOut, _ = t.PrepareDBConn()
            if err != nil  {
                panic(err)
            }

            err = stmtOut.QueryRow(1).Scan(&t.InsertedRow) // WHERE number = 1
        } else{
            panic(err.Error()) // proper error handling instead of panic in your app
        }
    }
}

func (t *TiProxyTest)PrepareDBConn() (error, *sql.Stmt, *sql.Stmt) {
    db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", (*t.Args).DBUser, (*t.Args).DBPassword, (*t.Args).DBHost, (*t.Args).DBPort, (*t.Args).DBName))
    if err != nil {
        panic(err)
    }

    db.SetConnMaxLifetime(time.Minute * 3)
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(10)

    stmtIns, err := db.Prepare("INSERT INTO tiproxy_test VALUES( ?, ?, ? )") // ? = placeholder
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    stmtSelHost, err := db.Prepare("select @@hostname") // ? = placeholder
    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    return nil, stmtIns , stmtSelHost
}

func (t *TiProxyTest)SearchSessionInfo(sessionID int) *SessionInfo {
    for _, sessionInfo := range t.LstSessionInfo {
        if sessionID == (*sessionInfo).ID {
            return sessionInfo
        }
    }
    var sessionInfo SessionInfo
    sessionInfo.ID = sessionID
    t.LstSessionInfo = append(t.LstSessionInfo, &sessionInfo)

    return &sessionInfo
}

func (t *TiProxyTest)PrintSessionInfo(sessionID int, stmtSelHost *sql.Stmt) error {
    sessionInfo := t.SearchSessionInfo(sessionID)

    var hostName string
    err := stmtSelHost.QueryRow().Scan(&hostName) // WHERE number = 1
    if err != nil {
        return err
    }

    if hostName != (*sessionInfo).Hostname {
        (*sessionInfo).Hostname = hostName
        tableOutput := [][]string{{"SessionID", "Host Name" }}
 
        for _, sessionInfo := range t.LstSessionInfo {
            tableOutput = append(tableOutput, []string{ strconv.Itoa(sessionInfo.ID), sessionInfo.Hostname })
        }

        tui.PrintTable(tableOutput, true)
        fmt.Printf("\n")
    }
    return nil
}

func (t *TiProxyTest)DBInsert(sessionID, pid int, wg *sync.WaitGroup, progressBar *progress.Bar, displayMsg *string ) {
    defer wg.Done()

    err, stmtIns, stmtSelHost := t.PrepareDBConn()
    if err != nil {
        panic(err)
    }

    t.PrintSessionInfo(sessionID, stmtSelHost)
    // fmt.Printf("The host Name is <%#v> \n\n\n\n", t.LstSessionInfo)

    for i := 0; i < (t.Args).Rows ; i++ {
        if i%10000 == 0 {
            t.InsertedRow = i
            (*progressBar).UpdateDisplay(&progress.DisplayProps{
                Prefix: fmt.Sprintf(*displayMsg, t.InsertedRow, t.NumOfTiDBRestart, t.NumOfRetry ),
            })
            if err = t.PrintSessionInfo(sessionID, stmtSelHost); err != nil {
                if err.Error() == "invalid connection"{
                    err, stmtIns, stmtSelHost = t.PrepareDBConn()
	            if err != nil {
                        panic(err)
	            }
                    err = t.PrintSessionInfo(sessionID, stmtSelHost)
                    if err != nil {
                        panic(err)
                    }
                }
            }
        }
        _, err := stmtIns.Exec(pid, i, (i + i)) // Insert tuples (i, i + i)
        if err != nil {
            if err.Error() == "invalid connection"{
                fmt.Printf("Starting to re-connect the DB \n\n\n")
                err, stmtIns, _ = t.PrepareDBConn()
		if err != nil {
                    panic(err)
	        }
		i = i - 1
		LK.Lock()
                t.NumOfRetry = t.NumOfRetry + 1
                (*progressBar).UpdateDisplay(&progress.DisplayProps{
                    Prefix: fmt.Sprintf(*displayMsg, t.InsertedRow, t.NumOfTiDBRestart, t.NumOfRetry ),
                }) 
		LK.Unlock()

	    } else{
                panic(err.Error()) // proper error handling instead of panic in your app
	    }
        }
    }
}
