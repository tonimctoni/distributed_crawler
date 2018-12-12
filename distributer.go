package main;

import "encoding/json"
import "crypto/md5"
import "math/rand"
import "os/signal"
import "net/http"
import "syscall"
import "bytes"
import "time"
import "fmt"
import "os"
import "io"


type Distributer struct{
    client http.Client
    reservoir_addresses []string
    strings_to_distribute chan []string
}

func make_distributer(reservoir_addresses []string) Distributer{
    return Distributer{
        http.Client{Timeout: 5*time.Second},
        reservoir_addresses,
        make(chan []string),
    }
}

func reservoir_address_to_uri(reservoir_address string) string{
    return fmt.Sprintf("http://%s/api/reservoir", reservoir_address)
}

type StatusCodeNotOk struct{
    status_code int
}

func (s StatusCodeNotOk) Error() string{
    return fmt.Sprintf("Status code is %d", s.status_code)
}

type Finished struct{}

func distribute_strings(distributed_strings [][]string, strings []string) [][]string{
    for _,s:=range(strings){
        hash_bytes:=md5.Sum([]byte(s))
        hash:=uint64(0)
        for i:=uint(0);i<8;i++{
            hash|=uint64(hash_bytes[i])<<(i*8)
        }
        bucket:=hash%uint64(len(distributed_strings))
        distributed_strings[bucket]=append(distributed_strings[bucket], s)
    }

    return distributed_strings
}

func send_distributed_strings(client http.Client, reservoir_addresses []string, strings_to_send [][]string){
    for i,reservoir_address:=range reservoir_addresses{
        if len(strings_to_send[i])==0{
            continue
        }

        b:=&bytes.Buffer{}
        err:=json.NewEncoder(b).Encode(strings_to_send[i])
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }

        response, err:=client.Post(reservoir_address_to_uri(reservoir_address), "application/json", b)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }
        response.Body.Close()

        if response.StatusCode!=http.StatusOK{
            err=StatusCodeNotOk{response.StatusCode}
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }
    }
}

func (d Distributer) run_distributer(finished chan Finished){
    defer close(finished)

    distributed_strings:=make([][]string, len(d.reservoir_addresses))
    ticker:=time.NewTicker(10*time.Second)
    defer ticker.Stop()

    outer: for{
        select{
        case strings, open:=<-d.strings_to_distribute:
            if !open{
                send_distributed_strings(d.client, d.reservoir_addresses, distributed_strings)
                break outer
            }

            distributed_strings=distribute_strings(distributed_strings, strings)
        case <-ticker.C:
            send_distributed_strings(d.client, d.reservoir_addresses, distributed_strings)
            distributed_strings=make([][]string, len(d.reservoir_addresses))
        }
    }
}

func (d Distributer) ServeHTTP(w http.ResponseWriter, r *http.Request){
    if r.Method=="POST"{
        strings:=[]string{}
        err:=json.NewDecoder(r.Body).Decode(&strings)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            w.WriteHeader(http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        d.strings_to_distribute<-strings
    } else if r.Method=="GET"{
        reservoir_address:=d.reservoir_addresses[rand.Intn(len(d.reservoir_addresses))]
        r,err:=http.NewRequest("GET", reservoir_address_to_uri(reservoir_address), r.Body) // Shadowing FTW
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            return
        }

        response,err:=d.client.Do(r)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            return
        }
        defer response.Body.Close()

        for k:=range response.Header {
            w.Header().Set(k, response.Header.Get(k))
        } 
        io.Copy(w, response.Body)
    } else{
        http.NotFound(w,r)
    }
}



func main() {
    if len(os.Args)<3{
        fmt.Fprintln(os.Stderr, "Error: expects at least two parameters: An address to listen to and the addresses of the reservoirs")
        return
    }

    mux:=http.NewServeMux()
    server:=&http.Server{
        Addr: os.Args[1],
        ReadTimeout: 5*time.Second,
        WriteTimeout: 5*time.Second,
        IdleTimeout: 5*time.Second,
        Handler: mux,
    }

    sigterm:=make(chan os.Signal)
    signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
    go func(){
        <-sigterm
        server.Close()
    }()


    finished:=make(chan Finished)
    distributer:=make_distributer(os.Args[2:])
    go distributer.run_distributer(finished)

    mux.Handle("/api/reservoir", distributer)
    fmt.Println("Start")
    err:=server.ListenAndServe()
    fmt.Fprintln(os.Stderr, err)

    close(distributer.strings_to_distribute)
    <-finished
    fmt.Println("End")
}