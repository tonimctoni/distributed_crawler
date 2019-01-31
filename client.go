package main;

import "encoding/json"
import "sync/atomic"
import "crypto/md5"
import "os/signal"
import "io/ioutil"
import "net/http"
import "syscall"
import "net/url"
import "strings"
import "regexp"
import "bytes"
import "time"
import "sync"
import "fmt"
import "os"


type StatusCodeNotOk struct{
    status_code int
}

func (s StatusCodeNotOk) Error() string{
    return fmt.Sprintf("Status code is %d", s.status_code)
}

type ContentTypeNotHtlm struct{
    content_type string
}

func (c ContentTypeNotHtlm) Error() string{
    return "ContentTypeNotHtlm: is "+c.content_type
}

type Finished struct{}

type Client struct{
    client http.Client
    end_clients_flag *int64
    wg *sync.WaitGroup
    url_finder_re *regexp.Regexp
    urls_to_distribute chan []string
    distributer_finished chan Finished
}

func make_client() Client{
    return Client{
        http.Client{Timeout: 5*time.Second},
        new(int64),
        new(sync.WaitGroup),
        regexp.MustCompile("(?:href=|src=|url=)[\"']?([^\"' <>]*)"),
        make(chan []string),
        make(chan Finished),
    }
}

func reservoir_address_to_uri(reservoir_address string) string{
    return fmt.Sprintf("http://%s/api/reservoir", reservoir_address)
}

func downloader_address_to_uri(reservoir_address string) string{
    return fmt.Sprintf("http://%s/api/downloader", reservoir_address)
}

func (c Client) retrieve_urls_from_reservoir(reservoir_address string) ([]string, error){
    r, err:=c.client.Get(reservoir_address_to_uri(reservoir_address))
    if err!=nil{
        return nil, err
    }

    if r.StatusCode!=http.StatusOK{
        return nil, StatusCodeNotOk{r.StatusCode}
    }

    urls:=[]string{}
    err=json.NewDecoder(r.Body).Decode(&urls)
    if err!=nil{
        return nil, err
    }

    return urls, nil
}

func (c Client) send_to(uri string, to_send interface{}) error{
    b:=&bytes.Buffer{}
    err:=json.NewEncoder(b).Encode(to_send)
    if err!=nil{
        return err
    }

    r, err:=c.client.Post(uri, "application/json", b)
    if err!=nil{
        return err
    }

    if r.StatusCode!=http.StatusOK{
        return StatusCodeNotOk{r.StatusCode}
    }

    return nil
}

func (c Client) extract_urls(base_url_as_string string, content []byte) ([]string, error){
    urls:=make([]string, 0, 64)
    base_url_as_url, err:=url.Parse(base_url_as_string)
    if err!=nil{
        return nil, err
    }

    for _,potential_url:=range c.url_finder_re.FindAllSubmatch(content,-1){
        url, err:=url.Parse(string(potential_url[1]))
        if err!=nil{
            continue
        }

        url=base_url_as_url.ResolveReference(url)

        if !(url.Scheme=="http" || url.Scheme=="https"){
            continue
        }

        url.Fragment=""
        url.RawQuery=""
        url.ForceQuery=false
        urls=append(urls, url.String())
    }

    return urls, nil
}

func (c Client) get_html(url string) ([]byte, error){
    r, err := c.client.Get(url)
    if err!=nil{
        return nil, err
    }
    defer r.Body.Close()

    content_type:=r.Header.Get("Content-Type")
    if !strings.Contains(content_type, "text/html"){
        return nil, ContentTypeNotHtlm{content_type}
    }

    return ioutil.ReadAll(r.Body)
}

func (c Client) run_client(reservoir_addresses []string){
    defer c.wg.Done()
    outer: for{
        for _,reservoir_address:=range reservoir_addresses{
            if atomic.LoadInt64(c.end_clients_flag)!=0{
                break outer
            }

            urls, err:=c.retrieve_urls_from_reservoir(reservoir_address)
            if err!=nil{
                fmt.Fprintln(os.Stderr, "Error:", err)
                time.Sleep(1*time.Second)
                continue
            }

            for _,url:=range urls{
                html_content, err:=c.get_html(url)
                if err!=nil{
                    if _,ok:=err.(ContentTypeNotHtlm); !ok{
                        fmt.Fprintln(os.Stderr, "Error:", err)
                    }
                    continue
                }

                new_urls, err:=c.extract_urls(url, html_content)
                if err!=nil{
                    fmt.Fprintln(os.Stderr, "Error:", err)
                    continue
                }

                c.urls_to_distribute <- new_urls
            }
        }
    }
}

func (c Client) send_distributed_urls_to_reservoirs(reservoir_addresses []string, urls_to_send [][]string){
    for i,reservoir_address:=range reservoir_addresses{
        if len(urls_to_send[i])==0{
            continue
        }

        b:=&bytes.Buffer{}
        err:=json.NewEncoder(b).Encode(urls_to_send[i])
        if err!=nil{
            fmt.Fprintln(os.Stderr, "Error:", err)
            continue
        }

        response, err:=c.client.Post(reservoir_address_to_uri(reservoir_address), "application/json", b)
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


func distribute_urls(distributed_urls [][]string, urls []string) [][]string{
    for _,s:=range(urls){
        hash_bytes:=md5.Sum([]byte(s))
        hash:=uint64(0)
        for i:=uint(0);i<8;i++{
            hash|=uint64(hash_bytes[i])<<(i*8)
        }
        bucket:=hash%uint64(len(distributed_urls))
        distributed_urls[bucket]=append(distributed_urls[bucket], s)
    }

    return distributed_urls
}

func (c Client) run_distributer(reservoir_addresses []string){
    defer close(c.distributer_finished)

    distributed_urls:=make([][]string, len(reservoir_addresses))
    distributed_urls_empty:=true
    ticker:=time.NewTicker(10*time.Second)
    defer ticker.Stop()

    outer: for{
        select{
        case urls, open:=<-c.urls_to_distribute:
            if len(urls)!=0{
                distributed_urls=distribute_urls(distributed_urls, urls)
                distributed_urls_empty=false
            }

            if !open{
                if distributed_urls_empty==false{
                    c.send_distributed_urls_to_reservoirs(reservoir_addresses, distributed_urls)
                }
                break outer
            }

        case <-ticker.C:
            if distributed_urls_empty==false{
                c.send_distributed_urls_to_reservoirs(reservoir_addresses, distributed_urls)
                distributed_urls=make([][]string, len(reservoir_addresses))
                distributed_urls_empty=true
            }
        }
    }
}

func main() {
    reservoir_addresses:=strings.Fields(os.Getenv("DCRAWLER_RESERVOIR_ADDRESSES"))
    if len(reservoir_addresses)==0{
        fmt.Fprintln(os.Stderr, "Error:", "DCRAWLER_RESERVOIR_ADDRESSES is not set")
        return
    }

    client:=make_client()
    client.wg.Add(32)
    for i:=0;i<32;i++{
        go client.run_client(reservoir_addresses)
    }

    go client.run_distributer(reservoir_addresses)

    sigterm:=make(chan os.Signal)
    signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
    fmt.Println("Start")

    <-sigterm
    atomic.StoreInt64(client.end_clients_flag, 1)
    fmt.Println("End flag set")
    client.wg.Wait()
    fmt.Println("Clients finished")
    close(client.urls_to_distribute)
    <-client.distributer_finished
    fmt.Println("Distributer finished")
    fmt.Println("End")
}
