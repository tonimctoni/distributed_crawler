package main;


import "net/http/httptest"
import "encoding/json"
import "net/http"
import "testing"
import "strconv"
import "bytes"

func TestStringQueue(t *testing.T) {
    strq:=make_string_queue()
    if strq.pop()!="" {
        t.Error("error")
    }
    strq.push("1")
    strq.push("2")
    if strq.pop()!="1" || strq.pop()!="2" || strq.pop()!="" || strq.len!=0{
        t.Error("error")
    }

    bufflen:=len(strq.buffer)
    for i:=0;i<bufflen;i++{
        strq.push("a"+strconv.Itoa(i))
    }

    if len(strq.buffer)!=bufflen || strq.len!=bufflen{
        t.Error("error")
    }

    for i:=0;i<len(strq.buffer);i++{
        if strq.pop()!="a"+strconv.Itoa(i){
            t.Error("error")
        }
    }

    bufflen=len(strq.buffer)
    for i:=0;i<bufflen+1;i++{
        strq.push("b"+strconv.Itoa(i))
    }

    if len(strq.buffer)==bufflen || strq.len!=bufflen+1{
        t.Error("error")
    }

    for i:=0;i<bufflen+1;i++{
        if strq.pop()!="b"+strconv.Itoa(i){
            t.Error("error")
        }
    }
}



func TestServeHTTP(t *testing.T) {

    get:=func(strings []string){
        reservoir:=make_reservoir()

        req, err:=http.NewRequest("GET", "/api/reservoir", nil)
        if err!=nil{
            t.Error("error")
        }

        w:=httptest.NewRecorder()

        go func() {reservoir.get<-strings}()
        reservoir.ServeHTTP(w, req)

        resp:=w.Result()
        if resp.StatusCode!=200 || resp.Header.Get("Content-Type")!="application/json"{
            t.Error("error")
        }

        var retrieved_strings []string
        err=json.NewDecoder(resp.Body).Decode(&retrieved_strings)
        if err!=nil{
            t.Error("error")
        }

        if len(retrieved_strings)!=len(strings){
            t.Error("error")
        }

        for i:=0;i<len(strings);i++{
            if retrieved_strings[i]!=strings[i]{
                t.Error("error")
            }
        }
    }

    post:=func(strings []string){
        reservoir:=make_reservoir()

        b:=&bytes.Buffer{}
        json.NewEncoder(b).Encode(strings)
        req, err:=http.NewRequest("POST", "/api/reservoir", b)
        if err!=nil{
            t.Error("error")
        }

        w:=httptest.NewRecorder()
        go reservoir.ServeHTTP(w, req)

        retrieved_strings:=<-reservoir.post

        if len(retrieved_strings)!=len(strings){
             t.Error("error")
        }

        for i:=0;i<len(strings);i++{
            if retrieved_strings[i]!=strings[i]{
                t.Error("error")
            }
        }

        resp:=w.Result()
        if resp.StatusCode!=200{
            t.Error("error")
        }
    }

    head:=func(){
        reservoir:=make_reservoir()

        req, err:=http.NewRequest("HEAD", "/api/reservoir", nil)
        if err!=nil{
            t.Error("error")
        }

        w:=httptest.NewRecorder()

        reservoir.ServeHTTP(w, req)

        resp:=w.Result()
        if resp.StatusCode!=404{
            t.Error("error")
        }
    }

    get([]string{})
    get([]string{"asd", "qwe"})

    post([]string{})
    post([]string{"asd", "qwe"})

    head()
}

func are_to_files_equal(a,b ToFile) bool{
    if len(a.strings)!=len(b.strings){
        return false
    }

    if a.n!=b.n{
        return false
    }

    for i:=0;i<len(a.strings);i++{
        if a.strings[i]!=b.strings[i]{
            return false
        }
    }

    return true;
}

func TestReservoir(t *testing.T){
    reservoir:=make_reservoir()
    end_reservoir:=make(chan End)
    reservoir.wg.Add(1)
    go reservoir.run_reservoir([]string{"a", "b", "c"}, 2, 10, end_reservoir)

    strings:=<-reservoir.get
    if len(strings)!=1 || strings[0]!="c"{
        t.Error("error")
    }

    strings=<-reservoir.get
    if len(strings)!=0{
        t.Error("error")
    }

    reservoir.post<-[]string{"b", "c", "d", "e"}
    strings=<-reservoir.get
    if len(strings)!=0{
        t.Error("error")
    }

    strings=<-reservoir.get
    if len(strings)!=2 || strings[0]!="d" || strings[1]!="e"{
        t.Error("error")
    }

    strings=<-reservoir.get
    if len(strings)!=0{
        t.Error("error")
    }

    reservoir.post<-[]string{"f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"}
    strings=<-reservoir.get
    if len(strings)!=0{
        t.Error("error")
    }

    strings=<-reservoir.get
    if len(strings)!=10{
        t.Error("error")
    }

    strings=<-reservoir.get
    if len(strings)!=2{
        t.Error("error")
    }

    close(end_reservoir)

    to_files:=[]ToFile{
        ToFile{[]string{}, 2},
        ToFile{[]string{}, 1},
        ToFile{[]string{"d", "e"}, 0},
        ToFile{[]string{}, 2},
        ToFile{[]string{"f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"}, 0},
        ToFile{[]string{}, 10},
        ToFile{[]string{}, 2},
    }

    for i:=0;i<len(to_files);i++{
        if !are_to_files_equal(<-reservoir.to_file,to_files[i] ){
            t.Error("error")
        }
    }
}
