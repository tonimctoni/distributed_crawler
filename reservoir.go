package main;

import "encoding/json"
import "io/ioutil"
import "os/signal"
import "net/http"
import "syscall"
import "strconv"
import "strings"
import "sync"
import "time"
import "fmt"
import "os"

type StringQueue struct{
    buffer []string
    end int
    len int
}

func make_string_queue() StringQueue{
    return StringQueue{make([]string, 2), 0, 0}
}

func (s *StringQueue) push(ns string){
    if s.len==len(s.buffer){
        new_buffer:=make([]string, len(s.buffer)*2)
        copy(new_buffer, s.buffer[:s.end])
        copy(new_buffer[len(new_buffer)-(s.len-s.end):], s.buffer[len(s.buffer)-(s.len-s.end):])
        s.buffer=new_buffer
    }

    s.buffer[s.end]=ns
    s.end+=1
    s.end%=len(s.buffer)
    s.len+=1
}

// func (s *StringQueue) pop() string{
//     if s.len==0{
//         return ""
//     }

//     if s.end==0{
//         s.end=len(s.buffer)
//     }

//     s.end-=1
//     s.len-=1
//     tmp:=s.buffer[s.end]
//     s.buffer[s.end]=""
//     return tmp
// }

func (s *StringQueue) pop() string{
    if s.len==0{
        return ""
    }

    pos:=(len(s.buffer)+s.end-s.len)%len(s.buffer)
    s.len-=1
    tmp:=s.buffer[pos]
    s.buffer[pos]=""
    return tmp
}

type End struct{}

type ToFile struct{
    strings []string
    n uint
}

type Reservoir struct{
    post chan []string
    get chan []string
    to_file chan ToFile
    wg *sync.WaitGroup
}

func make_reservoir() Reservoir{
    return Reservoir{
        make(chan []string),
        make(chan []string),
        make(chan ToFile, 1024),
        new(sync.WaitGroup),
    }
}

func (re Reservoir) ServeHTTP(w http.ResponseWriter, r *http.Request){
    if r.Method=="POST"{
        var strings []string
        err:=json.NewDecoder(r.Body).Decode(&strings)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            w.WriteHeader(http.StatusInternalServerError)
            return
        }
        w.WriteHeader(http.StatusOK)
        re.post <- strings
    } else if r.Method=="GET"{
        w.Header().Set("Content-Type", "application/json")
        err:=json.NewEncoder(w).Encode(<-re.get)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
        }
    } else{
        http.NotFound(w,r)
    }
}

func (re Reservoir) run_reservoir(initial_strings []string, n_discard uint, max_get uint, end chan End){
    defer re.wg.Done()
    defer close(re.to_file)
    to_get:=make([]string,0,max_get)
    already_added:=make(map[string]bool)
    for _,s:=range initial_strings{
        already_added[s]=true
    }

    if n_discard!=0{
        initial_strings=initial_strings[n_discard:]
        re.to_file<-ToFile{[]string{}, n_discard}
    }

    reservoir:=make_string_queue()
    for _,s:=range initial_strings{
        reservoir.push(s)
    }

    for i:=uint(0);i<max_get && reservoir.len!=0;i++{
        to_get=append(to_get, reservoir.pop())
    }

    outer: for{
        select{
        case new_strings:= <- re.post:
            to_file_strings:=make([]string,0,len(new_strings))
            for _,s:=range new_strings{
                if !already_added[s]{
                    already_added[s]=true
                    reservoir.push(s)
                    to_file_strings=append(to_file_strings, s)
                }
            }
            if len(to_file_strings)!=0{
                re.to_file<-ToFile{to_file_strings, 0}
            }
        case re.get <- to_get:
            if len(to_get)!=0{
                re.to_file<-ToFile{[]string{}, uint(len(to_get))}
            }
            to_get=make([]string,0,max_get)
            for i:=uint(0);i<max_get && reservoir.len!=0;i++{
                to_get=append(to_get, reservoir.pop())
            }
        case <- end:
            break outer
        }
    }
}

func (re Reservoir) run_file_writer(){
    defer re.wg.Done()

    fstrings, err:=os.OpenFile("strings.txt", os.O_APPEND|os.O_WRONLY, 0600)
    if err!=nil{
        fmt.Fprintln(os.Stderr, "Error:", err)
        panic("")
        return
    }
    defer fstrings.Close()

    fn, err:=os.Create("n.txt")
    if err!=nil{
        fmt.Fprintln(os.Stderr, "Error:", err)
        panic("")
        return
    }
    defer fn.Close()
    _,err=fn.WriteString("0")
    if err!=nil{
        fmt.Fprintln(os.Stderr, "Error:", err)
    }

    to_discard:=uint(0)
    for to_file:=range re.to_file{
        for _,s:=range to_file.strings{
            err:=error(nil)
            _,err=fstrings.Write([]byte(s))
            if err!=nil{
                fmt.Fprintln(os.Stderr, "Error:", err)
            }
            _,err=fstrings.Write([]byte("\n"))
            if err!=nil{
                fmt.Fprintln(os.Stderr, "Error:", err)
            }
        }

        if to_file.n!=0{
            to_discard+=to_file.n
            n,err:=fn.WriteAt([]byte(strconv.Itoa(int(to_discard))),0)
            if err!=nil{
                fmt.Fprintln(os.Stderr, "Error:", err)
            }

            err=fn.Truncate(int64(n))
            if err!=nil{
                fmt.Fprintln(os.Stderr, "Error:", err)
            }
        }
    }
}

func read_list_file(filename string) ([]string, error){
    content, err:=ioutil.ReadFile(filename)
    if err!=nil{
        return nil, err
    }

    lines:=strings.Split(string(content), "\n")
    return_strings:=make([]string, 0, len(lines))
    for _,line :=range lines{
        trimmed_line:=strings.TrimSpace(line)
        if len(trimmed_line)==0{
            continue
        }

        return_strings=append(return_strings, trimmed_line)
    }

    return return_strings, nil
}

func main() {
    if len(os.Args)!=2{
        fmt.Fprintln(os.Stderr, "Error: Expects address to listen to as only parameter")
        return
    }

    strings, err:=read_list_file("strings.txt")
    if err!=nil{
        fmt.Fprintln(os.Stderr, "Error:", err)
        return
    }

    to_discard:=uint64(0)
    b,err:=ioutil.ReadFile("n.txt")
    if err==nil{
        to_discard,err=strconv.ParseUint(string(b), 10, 64)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            return
        }
    }

    reservoir:=make_reservoir()
    reservoir.wg.Add(2)
    end_reservoir:=make(chan End)
    go reservoir.run_reservoir(strings,uint(to_discard),32,end_reservoir)
    go reservoir.run_file_writer()

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

    mux.Handle("/api/reservoir", reservoir)
    fmt.Println("Start")
    err=server.ListenAndServe()
    fmt.Fprintln(os.Stderr, err)

    close(end_reservoir)
    reservoir.wg.Wait()
    fmt.Println("End")
}