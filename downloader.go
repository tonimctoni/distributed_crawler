package main;

import "encoding/json"
import "crypto/md5"
import "io/ioutil"
import "os/signal"
import "net/http"
import "syscall"
import "strings"
import "sync"
import "time"
import "fmt"
import "os"



type Hash [md5.Size]byte

func (h *Hash) Equals(rhs *Hash) bool{
    return  h[0]==rhs[0] && h[1]==rhs[1] && h[2]==rhs[2] && h[3]==rhs[3] &&
            h[4]==rhs[4] && h[5]==rhs[5] && h[6]==rhs[6] && h[7]==rhs[7] &&
            h[8]==rhs[8] && h[9]==rhs[9] && h[10]==rhs[10] && h[11]==rhs[11] &&
            h[12]==rhs[12] && h[13]==rhs[13] && h[14]==rhs[14] && h[15]==rhs[15]
}

type HashSetPart struct{
    next_parts *[256]HashSetPart
    hash *Hash
}

type HashSet HashSetPart

func load_hash_set(filename string) (HashSet, int){
    f,err:=os.Open(filename)
    if err!=nil{
        return HashSet{}, 0
    }
    defer f.Close()

    hash_set:=HashSet{}
    count:=0
    for{
        hash:=new(Hash)
        n,err:=f.Read(hash[:])
        if n==0{
            break
        }

        if err!=nil{
            panic(err.Error())
        }

        if n!=md5.Size{
            panic("load_hash_set n!=md5.Size")
        }

        hash_set.ContainsAdd(hash)
        count+=1
    }

    return hash_set, count
}

func (h *HashSet) Contains(hash *Hash) bool{
    current_part:=(*HashSetPart)(h)
    for i:=0;i<md5.Size;i++{
        if current_part.hash!=nil{
            return current_part.hash.Equals(hash)
        }

        if current_part.next_parts==nil{
            return false
        }

        current_part=&current_part.next_parts[hash[i]]
    }

    panic("HashSet.Contains reached end")
}

// Consumes hash. Hash should NEVER be changed after
func (h *HashSet) ContainsAdd(hash *Hash) bool{
    current_part:=(*HashSetPart)(h)
    for i:=0;i<md5.Size;i++{
        if current_part.hash!=nil{
            if current_part.hash.Equals(hash){
                return true
            }
            current_part.next_parts=&[256]HashSetPart{}
            current_part.next_parts[current_part.hash[i]].hash=current_part.hash
            current_part.hash=nil
        }

        if current_part.next_parts==nil{
            current_part.hash=hash
            return false
        }

        current_part=&current_part.next_parts[hash[i]]
    }

    panic("HashSet.ContainsAdd reached end")
}

func get_extension_from_content_type(content_type string) string{
    content_types:=[...]string{
    "text/html",
    "text/css",
    "application/javascript",
    "text/plain",
    "image/bmp",
    "image/gif",
    "image/x-icon",
    "image/jpeg",
    "image/png",
    "video/webm",
    "audio/webm",
    "audio/wav",
    "video/mp4",
    "video/x-msvideo",
    "video/mpeg",
    "video/ogg",
    }

    extensions:=[...]string{"html", "css", "js", "txt", "bmp", "gif", "x-icon", "jpeg", "png", "webm", "weba", "wav", "mp4", "avi", "mpeg", "ogg"}

    for i,ct:=range content_types{
        if strings.Contains(content_type, ct){
            return extensions[i]
        }
    }

    return "other"
}

type FileContent struct{
    extension string
    content []byte
}

type Downloader struct{
    client http.Client
    incomming_urls chan string
    incomming_files chan FileContent
    wg *sync.WaitGroup
}

func make_downloader() Downloader{
    return Downloader{
        http.Client{Timeout: 5*time.Second},
        make(chan string),
        make(chan FileContent),
        new(sync.WaitGroup),
    }
}

func (d Downloader) ServeHTTP(w http.ResponseWriter, r *http.Request){
    if r.Method=="POST"{
        s:=""
        err:=json.NewDecoder(r.Body).Decode(&s)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            w.WriteHeader(http.StatusInternalServerError)
            return
        }
        w.WriteHeader(http.StatusOK)
        d.incomming_urls<-s
    } else{
        http.NotFound(w,r)
    }
}

func (d Downloader) run_downloader(){
    defer d.wg.Done()
    for url:=range d.incomming_urls{
        r, err:=http.Get(url)
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }

        b, err:=ioutil.ReadAll(r.Body)
        r.Body.Close()
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }

        d.incomming_files<-FileContent{get_extension_from_content_type(r.Header.Get("Content-Type")), b}
    }
}

func (d Downloader) run_file_writer(){
    defer d.wg.Done()
    hash_set, count:=load_hash_set("hashes.ash")

    fhashes, err:=os.OpenFile("hashes.ash", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    if err!=nil{
        fmt.Fprintln(os.Stderr, "Error:", err)
        panic("")
        return
    }
    defer fhashes.Close()

    for file_content:=range d.incomming_files{
        hash:=new(Hash)
        *hash=Hash(md5.Sum(file_content.content))
        if hash_set.ContainsAdd(hash){
            continue
        }

        ioutil.WriteFile(fmt.Sprintf("%05d.%s", count, file_content.extension), file_content.content, 0644)
        count+=1

        _,err:=fhashes.Write(hash[:])
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }
    }
}

func main() {
    if len(os.Args)!=2{
        fmt.Fprintln(os.Stderr, "Error: Expects address to listen to as only parameter")
        return
    }

    downloader:=make_downloader()
    downloader.wg.Add(6)
    go downloader.run_file_writer()
    for i:=0;i<5;i++{
        go downloader.run_downloader()
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

    mux.Handle("/api/downloader", downloader)
    fmt.Println("Start")
    err:=server.ListenAndServe()
    fmt.Fprintln(os.Stderr, err)

    close(downloader.incomming_urls)
    close(downloader.incomming_files)
    downloader.wg.Wait()
    fmt.Println("End")
}