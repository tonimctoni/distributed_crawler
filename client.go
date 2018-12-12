package main;

import "encoding/json"
import "sync/atomic"
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

type Client struct{
    client http.Client
    wg *sync.WaitGroup
    url_finder_re *regexp.Regexp
}

func make_client() Client{
    return Client{
        http.Client{Timeout: 5*time.Second},
        new(sync.WaitGroup),
        regexp.MustCompile("(?:href=|src=|url=)[\"']?([^\"' <>]*)"),
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

// func (c Client) send_urls_to_reservoir(reservoir_address string, urls []string) error{
//     b:=&bytes.Buffer{}
//     err:=json.NewEncoder(b).Encode(urls)
//     if err!=nil{
//         return err
//     }

//     r, err:=c.client.Post(reservoir_address_to_uri(reservoir_address), "application/json", b)
//     if err!=nil{
//         return err
//     }

//     if r.StatusCode!=http.StatusOK{
//         return StatusCodeNotOk{r.StatusCode}
//     }

//     return nil
// }

// func (c Client) send_url_to_downloader(downloader_address string, url string) error{
//     b:=&bytes.Buffer{}
//     err:=json.NewEncoder(b).Encode(url)
//     if err!=nil{
//         return err
//     }

//     r, err:=c.client.Post(downloader_address_to_uri(downloader_address), "application/json", b)
//     if err!=nil{
//         return err
//     }

//     if r.StatusCode!=http.StatusOK{
//         return StatusCodeNotOk{r.StatusCode}
//     }

//     return nil
// }

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

func (c Client) run_client(reservoir_addresses []string, downloader_address string, end *int64){
    defer c.wg.Done()
    outer: for{
        for _,reservoir_address:=range reservoir_addresses{
            if atomic.LoadInt64(end)!=0{
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
                    if ctnh,ok:=err.(ContentTypeNotHtlm); ok{
                        if strings.Contains(ctnh.content_type, "image/png"){
                            err=c.send_to(downloader_address_to_uri(downloader_address), url)
                            if err!=nil{
                                fmt.Fprintln(os.Stderr, "Error:", err)
                                continue
                            }
                        }
                    }else{
                        fmt.Fprintln(os.Stderr, "Error:", err)
                    }
                    continue
                }

                new_urls, err:=c.extract_urls(url, html_content)
                if err!=nil{
                    fmt.Fprintln(os.Stderr, "Error:", err)
                    continue
                }

                err=c.send_to(reservoir_address_to_uri(reservoir_address), new_urls)
                if err!=nil{
                    fmt.Fprintln(os.Stderr, "Error:", err)
                }
            }
        }
    }
}

func main() {
    if len(os.Args)<2{
        fmt.Fprintln(os.Stderr, "Error: expects at least two addresses as parameter: the downloader address and the addresses of the reservoirs")
        return
    }

    end:=new(int64)
    client:=make_client()
    client.wg.Add(32)
    for i:=0;i<32;i++{
        go client.run_client(os.Args[2:], os.Args[1], end)
    }

    sigterm:=make(chan os.Signal)
    signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
    fmt.Println("Start")

    <-sigterm
    atomic.StoreInt64(end, 1)
    fmt.Println("End flag set")
    client.wg.Wait()
    fmt.Println("End")
}
