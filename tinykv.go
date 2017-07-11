package main

import (
    "net"
    "fmt"
    "log"
    "bufio"
    "regexp"
    "strings"
)

type queueItem struct {
    k string
    v string
}

var QUEUE_BUFFER_SIZE = 10
var commandPattern = regexp.MustCompile("^(PUT|GET|DEL|LIST)")
var putPattern = regexp.MustCompile("^PUT [\x00-\x7F]{1,255} [\x00-\x7F]{1,255}$")
var getPattern = regexp.MustCompile("^GET [\x00-\x7F]{1,255}$")
var delPattern = regexp.MustCompile("^DEL [\x00-\x7F]{1,255}$")
var listPattern = regexp.MustCompile("^LIST$")
var kvStore = make(map[string]string)
var writeQueue = make(chan queueItem, QUEUE_BUFFER_SIZE)

func processQueue() {
    for {
        item := <-writeQueue
        if item.v != "" {
            kvStore[item.k] = item.v
        } else {
            delete(kvStore, item.k)
        }
    }
}

func handleClient(conn net.Conn) {
    for {
        message, err := bufio.NewReader(conn).ReadString('\n')
        if err != nil {
            if err.Error() == "EOF" {
                log.Printf("Client %s disconnected.", conn.RemoteAddr())
            } else {
                log.Printf("Read error from client %s - ", conn.RemoteAddr(), err.Error())
            }
            conn.Close()
            break
        }

        message = strings.TrimSpace(message)
        if !commandPattern.MatchString(message) {
            log.Printf("Invalid command: %s.", message)
            conn.Close()
            break
        }

        log.Printf("%s - %s", conn.RemoteAddr(), message)
        messageComponents := strings.Split(message, " ")
        switch messageComponents[0] {
            case "PUT":
                if !putPattern.MatchString(message) {
                    log.Printf("Invalid command: %s.", message)
                    conn.Close()
                    break
                }

                writeQueue <- queueItem{k: messageComponents[1], v: messageComponents[2] }
            case "GET":
                if !getPattern.MatchString(message) {
                    log.Printf("Invalid command: %s.", message)
                    conn.Close()
                    break
                }
                conn.Write([]byte(fmt.Sprintf("%s\n", kvStore[messageComponents[1]])))
           case "DEL":
                if !delPattern.MatchString(message) {
                    log.Printf("Invalid command: %s.", message)
                    conn.Close()
                    break
                }
                writeQueue <- queueItem{k: messageComponents[1], v: "" }
           case "LIST":
                if !listPattern.MatchString(message) {
                    log.Printf("Invalid command: %s.", message)
                    conn.Close()
                    break
                }
                for k, v := range kvStore {
                    conn.Write([]byte(fmt.Sprintf("%s: %s\n", k, v)))
                }
        }
    }
}

func main() {
    listener, err := net.Listen("tcp", "127.0.0.1:9999")
    if err != nil {
        log.Fatal(err.Error())
    }

    go processQueue()

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Print(err.Error())
            continue
        }

        log.Print("New connection from: ", conn.RemoteAddr())
        go handleClient(conn)
    }
}
